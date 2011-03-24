require 'date'
require 'json'
require 'sonar_connector'
require 'rfc822_util'
require 'base64'
require 'md5'
require 'rews'

module Sonar
  module Connector
    class EwsPullConnector < Sonar::Connector::Base

      MIN_BATCH_SIZE = 2
      DEFAULT_BATCH_SIZE = 100

      attr_accessor :url
      attr_accessor :auth
      attr_accessor :user
      attr_accessor :password
      attr_accessor :distinguished_folders
      attr_accessor :batch_size
      attr_accessor :delete
      attr_accessor :is_journal

      def parse(settings)
        ["name", "repeat_delay", "url", "auth", "user", "password", "distinguished_folders", "batch_size"].each do |param|
          raise Sonar::Connector::InvalidConfig.new("#{self.class}: param '#{param}' is blank") if settings[param].blank?
        end
        
        @url = settings["url"]
        @auth = settings["auth"]
        @user = settings["user"]
        @password = settings["password"]
        @mailbox_email = settings["mailbox_email"]
        @distinguished_folders = settings["distinguished_folders"]
        @batch_size = [settings["batch_size"] || DEFAULT_BATCH_SIZE, MIN_BATCH_SIZE].max
        @delete = !!settings["delete"]
        @is_journal = !!settings["is_journal"]
      end
      
      def inspect
        "#<#{self.class} @url=#{url}, @auth=#{auth}, @user=#{user}, @password=#{password}, @distinguished_folders=#{distinguished_folders}, @batch_size=#{batch_size}, @delete=#{delete}, @is_journal=#{is_journal}>"
      end

      def distinguished_folder_ids
        return @distinguished_folder_ids if @distinguished_folder_ids
        client ||= Rews::Client.new(url, auth, user, password)

        @distinguished_folder_ids = @distinguished_folders.inject([]) do |ids, (name, mailbox_email)|
          ids << client.distinguished_folder_id(name, mailbox_email)
        end
      end

      # find message ids from a folder
      def find(folder_id, offset)
        find_opts = {
          :sort_order=>[["item:DateTimeReceived", "Ascending"]],
          :indexed_page_item_view=>{
            :max_entries_returned=>batch_size, 
            :offset=>offset},
          :item_shape=>{
            :base_shape=>:IdOnly}}
        
        restriction = [:==, "item:ItemClass", "IPM.Note"]
        if state[folder_id.key]
          restriction = [:and,
                         restriction,
                         [:>= , "item:DateTimeReceived", state[folder_id.key]]]
        end
        find_opts[:restriction] =  restriction
        
        folder_id.find_item(find_opts)
      end

      def get(folder_id, msg_ids)
        get_opts = {
          :item_shape=>{
            :base_shape=>:IdOnly, 
            :additional_properties=>[[:field_uri, "item:ItemClass"],
                                     [:field_uri, "item:DateTimeSent"],
                                     [:field_uri, "item:DateTimeReceived"],
                                     [:field_uri, "item:InReplyTo"],
                                     [:field_uri, "message:InternetMessageId"],
                                     [:field_uri, "message:References"],
                                     [:field_uri, "message:From"],
                                     [:field_uri, "message:Sender"],
                                     [:field_uri, "message:ToRecipients"],
                                     [:field_uri, "message:CcRecipients"],
                                     [:field_uri, "message:BccRecipients"]]}}

        # we have to retrieve the journal message content and unwrap the 
        # original message if this
        # is an exchange journal mailbox
        if is_journal
          get_opts[:item_shape][:additional_properties] << [:field_uri, "item:MimeContent"]
        end
        
        folder_id.get_item(msg_ids, get_opts)
      end

      def action
        distinguished_folder_ids.each do |fid|
          log.info "processing: #{fid.inspect}"

          offset = 0

          begin
            msg_ids = find(fid, offset)

            if msg_ids && msg_ids.length>0
              msgs = get(fid, msg_ids)

              # if there is no state, then state is set to the first message timestamp
              state[fid.key] ||= msgs.first[:date_time_received].to_s if msgs.first[:date_time_received]

              if msgs.last[:date_time_received] != state[fid.key]
                finished=true
                state[fid.key] = msgs.last[:date_time_received].to_s
              end
              
              save_messages(msgs)
              delete_messages(fid, msgs) if delete

              offset += msg_ids.length
            end
          end while msg_ids.length>0 && !finished

          save_state

          log.info "finished processing: #{fid.inspect}"
        end
        log.info "finished action"
      end

      def save_messages(messages)
        messages.each do |msg|
          if is_journal
            h = extract_journalled_message(msg)
          else
            h = message_to_hash(msg)
          end
          h[:type] = "email"
          h[:connector] = name
          h[:source] = url
          h[:source_id]=msg[:item_id][:id]
          h[:received_at] = msg[:date_time_received]


          fname = MD5.hexdigest(msg[:item_id][:id])
          filestore.write(:complete, "#{fname}.json", h.to_json)
        end
      end

      def mailbox_to_hash(mailbox)
        [:name, :email_address].inject({}) do |h, k|
          h[k] = mailbox[k]
          h
        end
      end

      def mailbox_recipients_to_hashes(recipients)
        mailboxes = recipients[:mailbox] if recipients
        mailboxes = [mailboxes] if !mailboxes.is_a?(Array)
        mailboxes.compact.map{|a| mailbox_to_hash(a)}
      end

      def message_to_hash(msg)
        message_id = Rfc822Util.strip_header(msg[:internet_message_id]) if msg[:internet_message_id]
        in_reply_to = Rfc822Util.strip_headers(msg[:in_reply_to]).first if msg[:in_reply_to]
        references = Rfc822Util.strip_headers(msg[:references]) if msg[:references]
        
          json_hash = {
            :message_id=>message_id,
            :sent_at=>msg[:date_time_sent].to_s,
            :in_reply_to=>in_reply_to,
            :references=>references,
            :from=>mailbox_recipients_to_hashes(msg[:from]).first,
            :sender=>mailbox_recipients_to_hashes(msg[:sender]).first,
            :to=>mailbox_recipients_to_hashes(msg[:to_recipients]),
            :cc=>mailbox_recipients_to_hashes(msg[:cc_recipients]),
            :bcc=>mailbox_recipients_to_hashes(msg[:bcc_recipients])
          }
          
      end

      def extract_journalled_message(message)
        mime_msg = Base64::decode64(message[:mime_content])
        journal_msg = Rfc822Util.extract_journalled_mail(mime_msg)
        Rfc822Util.mail_to_hash(journal_msg)
      end

      def delete_messages(folder_id, messages)
        log.info "deleting #{messages.length} messages from #{folder_id.inspect}"
        folder_id.delete_item(messages, :delete_type=>:HardDelete)
      end
    end
  end
end
