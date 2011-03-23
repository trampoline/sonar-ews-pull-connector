require 'date'
require 'json'
require 'sonar_connector'
require 'exjournal'
require 'rews'

module Sonar
  module Connector
    class EwsPullConnector < Sonar::Connector::Base

      MIN_BATCH_SIZE = 2
      DEFAULT_BATCH_SIZE = 100

      attr_reader :url
      attr_reader :auth
      attr_reader :user
      attr_reader :password
      attr_reader :distinguished_folders
      attr_reader :batch_size
      attr_reader :delete
      attr_reader :is_journal

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
        "#<#{self.class} @url=#{url}, @auth=#{auth}, @user=#{user}, @password=#{password}, @distinguished_folders=#{distinguished_folders}, @batch_size=#{batch_size}>"
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
              state[fid.key] ||= msgs.first[:date_time_received] if msgs.first[:date_time_received]

              if msgs.last[:date_time_received] > state[fid.key]
                finished=true
                state[fid.key] = msgs.last[:date_time_received]
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
          h[:source_id]=msg[:item_id]
          h[:received_at] = msg[:date_time_received]

          filestore.write(:complete, "#{msg[:item_id][:id]}.json", h.to_json)
        end
      end

      def message_to_hash(msg)
          json_hash = {
            :message_id=>msg[:internet_message_id],
            :sent_at=>msg[:date_time_sent],
            :in_reply_to=>msg[:in_reply_to],
            :references=>msg[:references],
            :from=>msg[:from],
            :sender=>msg[:sender],
            :to=>msg[:to_recipients],
            :cc=>msg[:cc_recipients],
            :bcc=>msg[:bcc_recipients]
          }
          
      end

      def extract_journalled_message(message)
        mime_msg = message[:mime_content]
        journal_msg = Exjournal.extract_journalled_mail(mime_msg)
        Exjournal.mail_to_hash(journal_msg)
      end

      def delete_messages(folder_id, messages)
        log.info "deleting #{messages.length} messages from #{folder_id.inspect}"
        folder_id.delete_item(messages, :delete_type=>:HardDelete)
      end
    end
  end
end
