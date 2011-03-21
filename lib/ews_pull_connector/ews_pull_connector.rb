require 'date'
require 'json'
require 'sonar_connector'
require 'rews'

module Sonar
  module Connector
    class EwsPullConnector < Sonar::Connector::Base

      DEFAULT_BATCH_SIZE = 100

      attr_reader :url
      attr_reader :auth
      attr_reader :user
      attr_reader :password
      attr_reader :distinguished_folders
      attr_reader :batch_size
      attr_reader :delete

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
        @batch_size = settings["batch_size"] || DEFAULT_BATCH_SIZE
        @delete = settings["delete"]
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

      def action
        distinguished_folder_ids.each do |fid|
          log.info "processing: #{fid.inspect}"

          fstate = state[fid.key] if state[fid.key]

          offset = 0

          begin
            query_opts = {
              :sort_order=>[["item:DateTimeReceived", "Ascending"]],
              :indexed_page_item_view=>{
                :max_entries_returned=>batch_size, 
                :offset=>offset},
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
                                         [:field_uri, "message:BccRecipients"]]}
            }
            
            restriction = [:==, "item:ItemClass", "message"]
            if fstate
              restriction = [:and,
                             restriction,
                             [:>= , "item:DateTimeReceived", fstate]]
            end
            query_opts[:restriction] =  restriction
            
            msgs = fid.find_item(query_opts)

            save_messages(msgs)
            delete_messages(fid, msgs) if delete

            offset += msgs.size
          end while msgs.result.last["item:DateTimeReceived"].to_s == 
            (fstate || msgs.result.first["item:DateTimeReceived"].to_s)

          state[fid.key] = msgs.result.last["item:DateTimeReceived"].to_s
          save_state

          log.info "finished processing: #{fid.inspect}"
        end
        log.info "finished action"
      end

      def save_messages(messages)
        messages.result.each do |msg|
          json_hash = {
            :type=>"email",
            :connector=>name,
            :source=>url,
            :source_id=>msg[:item_id],
            :sent_at=>msg["item:DateTimeSent"],
            :received_at=>msg["item:DateTimeReceived"],
            :in_reply_to=>msg["item:InReplyTo"],
            :message_id=>msg["message:InternetMessageId"],
            :references=>msg["message:References"],
            :from=>msg["message:From"],
            :sender=>msg["message:Sender"],
            :to=>msg["message:ToRecipients"],
            :cc=>msg["message:CcRecipients"],
            :bcc=>msg["message:BccRecipients"]
          }
          
          filestore.write(:complete, "#{msg[:item_id][:id]}.json", json_hash.to_json)
        end
      end

      def delete_messages(folder_id, messages)
        log.info "deleting #{messages.length} messages from #{folder_id.inspect}"
        folder_id.delete_item(messages, :delete_type=>:HardDelete)
      end
    end
  end
end
