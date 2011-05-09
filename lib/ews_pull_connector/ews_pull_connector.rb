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

      attr_accessor :url
      attr_accessor :auth
      attr_accessor :user
      attr_accessor :password
      attr_accessor :distinguished_folders
      attr_accessor :delete
      attr_accessor :is_journal
      attr_accessor :batch_size
      attr_accessor :client

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
        @delete = !!settings["delete"]
        @is_journal = !!settings["is_journal"]
        @batch_size = settings["batch_size"]

        raise "batch_size must be >= 1" if batch_size<1
        raise "batch_size must be >= 2 if delete not true" if !delete && batch_size<2
        raise "batch_size must be 1 if is_journal is true" if is_journal && batch_size>1
      end
      
      def inspect
        "#<#{self.class} @url=#{url}, @auth=#{auth}, @user=#{user}, @password=#{password}, @distinguished_folders=#{distinguished_folders}, @batch_size=#{batch_size}, @delete=#{delete}, @is_journal=#{is_journal}>"
      end

      def distinguished_folder_ids
        return @distinguished_folder_ids if @distinguished_folder_ids
        self.client ||= Rews::Client.new(url, auth, user, password)

        @distinguished_folder_ids = @distinguished_folders.inject([]) do |ids, (name, mailbox_email)|
          ids << client.distinguished_folder_id(name, mailbox_email)
        end
      end

      # find message ids from a folder, including item:DateTimeReceived, ordered
      # by ascending item:DateTimeReceived
      def find(folder_id, offset)
        find_opts = {
          :sort_order=>[["item:DateTimeReceived", "Ascending"]],
          :indexed_page_item_view=>{
            :max_entries_returned=>batch_size, 
            :offset=>offset},
          :item_shape=>{
            :base_shape=>:IdOnly,
            :additional_properties=>[[:field_uri, "item:DateTimeReceived"],
                                     [:field_uri, "message:IsRead"],
                                     [:field_uri, "message:IsReadReceiptRequested"]]}}
        
        restriction = [:==, "item:ItemClass", "IPM.Note"]
        if state[folder_id.key]
          restriction = [:and,
                         restriction,
                         [:>= , "item:DateTimeReceived", state[folder_id.key]]]
        end
        find_opts[:restriction] =  restriction
        
        folder_id.find_item(find_opts)
      end

      def get(msg_ids)
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
                                     [:field_uri, "message:BccRecipients"],
                                     [:field_uri, "message:IsRead"],
                                     [:field_uri, "message:IsReadReceiptRequested"]]}}

        # we have to retrieve the journal message content and unwrap the 
        # original message if this
        # is an exchange journal mailbox
        if is_journal
          get_opts[:item_shape][:additional_properties] << [:field_uri, "item:MimeContent"]
        end
        
        client.get_item(msg_ids, get_opts)
      end

      def action
        distinguished_folder_ids.each do |fid|
          log.info "processing: #{fid.inspect}"

          offset = 0

          begin
            msg_ids = find(fid, offset)

            if msg_ids && msg_ids.length>0
              # if there is no state, then state is set to the first message timestamp
              state[fid.key] ||= msg_ids.first[:date_time_received].to_s if msg_ids.first[:date_time_received]

              begin
                msgs = get(msg_ids)
                save_messages(msgs)
              rescue Exception=>e
                log.warn("problem retrieving messages: #{msg_ids.inspect}")
                log.warn(e)
                log.warn("messages WILL be deleted") if delete
                save_error(msg_ids, e.savon_response) if e.respond_to?(:savon_response)
              end

              if delete || msg_ids.last[:date_time_received] != state[fid.key]
                finished=true
                state[fid.key] = msg_ids.last[:date_time_received].to_s
              end
              
              # need to suppress any requested read receipts, even if we delete
              suppress_read_receipt(msg_ids)
              delete_messages(msg_ids) if delete

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
          h = if is_journal
                extract_journalled_message(msg)
              else
                message_to_hash(msg)
              end
          
          if !h
            log.warn("no data extracted from message. could be a decoding eror")
            return
          end

          h[:connector] = name
          h[:source] = url
          h[:source_id]=msg[:item_id][:id]
          h[:received_at] = msg[:date_time_received]

          fname = MD5.hexdigest(msg[:item_id][:id])
          filestore.write(:complete, "#{fname}.json", h.to_json)
        end
      end

      def save_error(msg_ids, savon_response)
        xml = savon_response.to_xml
        fname = "error.xml"
        filestore.write(:error, fname, xml)
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
          :message_type=>"email",
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
      rescue Exception=>e
        log.warn("problem extracting journalled message from wrapper message")
        log.warn(e)
        nil
      end

      def suppress_read_receipt(messages)
        log.info "suppressing any read receipts"
        client.suppress_read_receipt(messages)
      end

      def delete_messages(messages)
        log.info "deleting #{messages.length} messages"
        client.delete_item(messages, :delete_type=>:HardDelete)
      end
    end
  end
end
