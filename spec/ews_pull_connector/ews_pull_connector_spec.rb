require File.expand_path("../../spec_helper", __FILE__)

module Sonar
  module Connector
    describe EwsPullConnector do

      before do
        setup_valid_config_file
        @base_config = Sonar::Connector::Config.load(valid_config_filename)
      end

      def one_folder_config(opts={})
        {
          'name'=>'foobarcom-exchange',
          'repeat_delay'=>60,
          'url'=>"https://foo.com/EWS/Exchange.asmx",
          'auth'=>'ntlm',
          'user'=>"foo",
          'password'=>"foopass",
          'distinguished_folders'=>[["inbox", "foo@foo.com"]],
          'batch_size'=>100,
          'delete'=>false
        }.merge(opts)
      end

      def two_folder_config(opts={})
        {
          'name'=>'foobarcom-exchange',
          'repeat_delay'=>60,
          'url'=>"https://foo.com/EWS/Exchange.asmx",
          'auth'=>'ntlm',
          'user'=>"foo",
          'password'=>"foopass",
          'distinguished_folders'=>[["inbox", "foo@foo.com"], "inbox"],
          'batch_size'=>100,
          'delete'=>false
        }.merge(opts)
      end

      it "should parse config" do
        Sonar::Connector::EwsPullConnector.new(two_folder_config, @base_config)
      end
      
      describe "distinguished_folder_ids" do
        it "should create Rews::Clients for each configured distinguished folder" do
          c=Sonar::Connector::EwsPullConnector.new(two_folder_config, @base_config)
          fids = c.distinguished_folder_ids
          fids.size.should == 2

          fids[0].client.should be(fids[1].client)
          client = fids[0].client
          client.endpoint.should == "https://foo.com/EWS/Exchange.asmx"
          client.auth_type.should == "ntlm"
          client.user.should == 'foo'
          client.password.should == 'foopass'
          
          fid0 = fids[0]
          fid0.id.should == 'inbox'
          fid0.mailbox_email.should == 'foo@foo.com'
          fid0.key.should == ['distinguished_folder', 'inbox', 'foo@foo.com']

          fid1 = fids[1]
          fid1.id.should == 'inbox'
          fid1.mailbox_email.should == nil 
          fid1.key.should == ['distinguished_folder', 'inbox']
        end

        it "should cache the Rews::Clients" do
          c=Sonar::Connector::EwsPullConnector.new(two_folder_config, @base_config)
          fids = c.distinguished_folder_ids
          fid0 = fids[0]
          fid1 = fids[1]

          fids = c.distinguished_folder_ids
          fid0.should be(fids[0])
          fid1.should be(fids[1])
        end
      end

      describe "find" do
        it "should include batch_size and offset but not item:DateTimeReceived restriction if there is no folder state" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          state={}
          stub(c.state){state}
          stub(c).batch_size{17}

          folder_id = Object.new
          folder_key = Object.new
          stub(folder_id).key{folder_key}

          mock(folder_id).find_item.with_any_args do |find_opts|
            find_opts.should == {
              :sort_order=>[["item:DateTimeReceived", "Ascending"]],
              :indexed_page_item_view=>{
                :max_entries_returned=>17, 
                :offset=>123},
              :item_shape=>{
                :base_shape=>:IdOnly,
                :additional_properties=>[[:field_uri, "item:DateTimeReceived"],
                                         [:field_uri, "message:IsRead"],
                                         [:field_uri, "message:IsReadReceiptRequested"]]},
              :restriction=>[:==, "item:ItemClass", "IPM.Note"]}
          end

          c.find(folder_id, 123)
        end

        it "should include a item:DateTimeReceived restriction if there is folder state" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          stub(c).batch_size{17}

          folder_id = Object.new
          folder_key = Object.new
          stub(folder_id).key{folder_key}

          state_time = DateTime.now.to_s
          stub(c).state.stub!.[](folder_key){state_time}

          mock(folder_id).find_item.with_any_args do |find_opts|
            find_opts.should == {
              :sort_order=>[["item:DateTimeReceived", "Ascending"]],
              :indexed_page_item_view=>{
                :max_entries_returned=>17, 
                :offset=>123},
              :item_shape=>{
                :base_shape=>:IdOnly,
                :additional_properties=>[[:field_uri, "item:DateTimeReceived"],
                                         [:field_uri, "message:IsRead"],
                                         [:field_uri, "message:IsReadReceiptRequested"]]},
              :restriction=>[:and,
                             [:==, "item:ItemClass", "IPM.Note"],
                             [:>=, "item:DateTimeReceived", state_time]]}
          end

          c.find(folder_id, 123)
        end
      end

      describe "get" do
        it "should not fetch message content if !is_journal" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)

          client = Object.new
          stub(c).client{client}
          msg_ids = Object.new

          mock(client).get_item.with_any_args do |mids, get_opts|
            mids.should be(msg_ids)
            get_opts.should == {
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
          end

          c.get(msg_ids)
        end

        it "should fetch message content if is_journal" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          stub(c).is_journal{true}

          client = Object.new
          stub(c).client{client}
          msg_ids = Object.new

          mock(client).get_item.with_any_args do |mids, get_opts|
            mids.should be(msg_ids)
            get_opts.should == {
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
                                         [:field_uri, "message:IsReadReceiptRequested"],
                                         [:field_uri, "item:MimeContent"]]}}
          end

          c.get(msg_ids)
        end
      end

      describe "action" do
        it "should make a Rews find_item request, save, update state, delete" do
          c=Sonar::Connector::EwsPullConnector.new(two_folder_config('delete'=>true), @base_config)
          state = {}
          stub(c).state{state}
          
          c.distinguished_folder_ids.each do |fid|
            msg_ids = Object.new
            stub(msg_ids).length{1}

            stub(msg_ids).first.stub!.[](:date_time_received){ DateTime.now-1 }
            stub(msg_ids).last.stub!.[](:date_time_received){ DateTime.now }

            mock(fid).find_item(anything){msg_ids}

            msgs = Object.new
            mock(c.client).get_item(msg_ids, anything){msgs}
            
            mock(c).save_messages(msgs)
            mock(c).suppress_read_receipt(msg_ids)
            mock(c).delete_messages(msg_ids)
          end
          c.action
        end

        it "should catch exceptions during message fetch and write any xml to an error file" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)

          fid = c.distinguished_folder_ids.first

          msg_ids = Object.new
          stub(msg_ids).length{10}
          
          earlier = DateTime.now - 1
          stub(msg_ids).first.stub!.[](:date_time_received){ earlier }
          later = DateTime.now
          stub(msg_ids).last.stub!.[](:date_time_received){ later }
          
          mock(fid).find_item(anything){msg_ids}
          
          exception = RuntimeError.new("boo")
          savon_response = Object.new
          stub(savon_response).to_xml{"<tig><tag/></tig>"}
          stub(exception).savon_response{savon_response}

          msgs = Object.new
          mock(c.client).get_item(msg_ids, anything){raise exception}
          mock(c.log).warn(/problem retrieving/)
          mock(c.log).warn(is_a(RuntimeError))
          
          dont_allow(c).save_messages(msgs)
          mock(c).suppress_read_receipt(msg_ids)

          mock(c.filestore).write(:error, "error.xml", "<tig><tag/></tig>")
          
          c.action

          c.state[fid.key].should == later.to_s
        end

        it "should catch exceptions during fetch and delete if 'delete' option is true" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config('delete'=>true), @base_config)
          fid = c.distinguished_folder_ids.first

          msg_ids = Object.new
          stub(msg_ids).length{10}
          
          earlier = DateTime.now - 1
          stub(msg_ids).first.stub!.[](:date_time_received){ earlier }
          later = DateTime.now
          stub(msg_ids).last.stub!.[](:date_time_received){ later }
          
          mock(fid).find_item(anything){msg_ids}
          
          msgs = Object.new
          mock(c.client).get_item(msg_ids, anything){raise "boo"}
          mock(c.log).warn(/problem retrieving/)
          mock(c.log).warn(is_a(RuntimeError))
          mock(c.log).warn(/be deleted/)
          
          dont_allow(c).save_messages(msgs)
          mock(c).suppress_read_receipt(msg_ids)
          mock(c).delete_messages(msg_ids)
          
          c.action

          c.state[fid.key].should == later.to_s
        end

        it "should have a single item:ItemClass Restriction clause if fstate is nil" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          msg_ids = Object.new
          stub(msg_ids).length{1}
          mock(fid).find_item(anything) do |query_opts|
            r = query_opts[:restriction]
            r.should == [:==, "item:ItemClass", "IPM.Note"] 
            msg_ids
          end

          stub(msg_ids).first.stub!.[](:date_time_received){ DateTime.now-1 }
          stub(msg_ids).last.stub!.[](:date_time_received){DateTime.now}

          msgs=Object.new
          mock(c.client).get_item(msg_ids, anything){msgs}
          mock(c).suppress_read_receipt(msg_ids)
          mock(c).save_messages(msgs)

          c.action
        end
        
        it "should have a second item:DateTimeReceived Restriction clause if fstate is non-nil" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          state_time = DateTime.now - 1
          state = {fid.key => state_time.to_s}
          stub(c).state{state}

          msg_ids = Object.new
          stub(msg_ids).length{1}
          mock(fid).find_item(anything) do |query_opts|
            r = query_opts[:restriction]
            r[0].should == :and
              r[1].should == [:==, "item:ItemClass", "IPM.Note"] 
            r[2].should == [:>=, "item:DateTimeReceived", state_time.to_s]
            msg_ids
          end

          stub(msg_ids).first.stub!.[](:date_time_received){state_time}
          stub(msg_ids).last.stub!.[](:date_time_received){DateTime.now}

          msgs = Object.new
          mock(c.client).get_item(msg_ids, anything){msgs}
          mock(c).suppress_read_receipt(msg_ids)
          mock(c).save_messages(msgs)

          c.action
        end

        it "should not cycle through messages with identical item:DateTimeRecieved if 'delete' option is true" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config('delete'=>true), @base_config)
          fid = c.distinguished_folder_ids.first

          state_time = (DateTime.now - 2).to_s
          state = {fid.key => state_time}
          stub(c).state{state}

          msg_ids = Object.new
          stub(msg_ids).length{10}
          stub(msg_ids).first.stub!.[](:date_time_received){state_time}
          stub(msg_ids).last.stub!.[](:date_time_received){state_time}
          msgs = Object.new

          mock(fid).find_item.with_any_args do |opts| 
            opts[:indexed_page_item_view][:offset].should == 0
            msg_ids
          end

          mock(c.client).get_item(msg_ids, anything){msgs}
          mock(c).suppress_read_receipt(msg_ids)
          mock(c).save_messages(msgs)
          mock(c).delete_messages(msg_ids)

          c.action
        end

        it "should cycle through messages with identical item:DateTimeReceived, so state is always updated if 'delete' option is false" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          state_time = (DateTime.now - 2).to_s
          state = {fid.key => state_time}
          stub(c).state{state}

          msg_ids = Object.new
          stub(msg_ids).length{10}
          stub(msg_ids).first.stub!.[](:date_time_received){state_time}
          stub(msg_ids).last.stub!.[](:date_time_received){state_time}
          msgs = Object.new

          more_msg_ids = Object.new
          stub(more_msg_ids).length{1}
          stub(more_msg_ids).first.stub!.[](:date_time_received){state_time}
          stub(more_msg_ids).last.stub!.[](:date_time_received){DateTime.now}
          more_msgs = Object.new

          mock(fid).find_item.with_any_args.twice do |opts| 
            if opts[:indexed_page_item_view][:offset]==0
              msg_ids
            elsif opts[:indexed_page_item_view][:offset]==10
              more_msg_ids
            else
              raise "oops"
            end
          end

          mock(c.client).get_item(msg_ids, anything){msgs}
          mock(c).suppress_read_receipt(msg_ids)
          mock(c).save_messages(msgs)

          mock(c.client).get_item(more_msg_ids, anything){more_msgs}
          mock(c).suppress_read_receipt(more_msg_ids)
          mock(c).save_messages(more_msgs)

          c.action
        end

        it "should terminate the fetch loop if no messages are returned" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          state_time = (DateTime.now - 1).to_s
          state = {fid.key => state_time}
          stub(c).state{state}

          msg_ids = Object.new
          stub(msg_ids).length{0}
          msgs = Object.new

          mock(fid).find_item.with_any_args do |opts| 
            opts[:indexed_page_item_view][:offset].should == 0
            msg_ids
          end

          c.action
        end


      end

      describe "mailbox_to_hash" do
        it "should keep only :name and :email_address keys of a Rews address hash" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          mb = {:name=>"foo bar", :email_address=>"foo@bar.com", :blah=>"blah"}
          c.mailbox_to_hash(mb).should == {:name=>"foo bar", :email_address=>"foo@bar.com"}
        end
      end

      describe "mailbox_recipients_to_hashes" do
        it "should convert a nil recipient to an empty list" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          mbr = {:mailbox=>nil}
          c.mailbox_recipients_to_hashes(mbr).should == []
        end

        it "should convert a single recipient to a list of one hash" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          mbr = {:mailbox=>{:name=>"foo bar", :email_address=>"foo@bar.com", :blah=>"blah"}}
          c.mailbox_recipients_to_hashes(mbr).should == [{:name=>"foo bar", :email_address=>"foo@bar.com"}]
        end

        it "should convert multiple recipients to a list of hashes" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          mbr = {:mailbox=>[{:name=>"foo bar", :email_address=>"foo@bar.com", :blah=>"blah"},
                            {:name=>"baz mcbaz", :email_address=>"baz.mcbaz@baz.com"}]}
          c.mailbox_recipients_to_hashes(mbr).should == [{:name=>"foo bar", :email_address=>"foo@bar.com"},
                                                         {:name=>"baz mcbaz", :email_address=>"baz.mcbaz@baz.com"}]
        end
      end

      describe "message_to_hash" do
        it "should convert a message Rews::Item::Item to a hash" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          sent_at = DateTime.now-1
          m = Rews::Item::Item.new(c, 
                                   :message, 
                                   :item_id=>{:id=>"abc", :change_key=>"def"},
                                   :internet_message_id=>"<abc123>",
                                   :date_time_sent=>sent_at,
                                   :date_time_received=>DateTime.now,
                                   :in_reply_to=>"<foo>",
                                   :references=>["<foo>", "<bar>"],
                                   :from=>{:mailbox=>{:name=>"foo mcfoo", :email_address=>"foo.mcfoo@foo.com"}},
                                   :sender=>{:mailbox=>{:name=>"mrs mcmrs", :email_address=>"mrs.mcmrs@foo.com"}},
                                   :to_recipients=>{:mailbox=>[{:name=>"bar mcbar", :email_address=>"bar.mcbar@bar.com"}, {:name=>"baz mcbaz", :email_address=>"baz.mcbaz@baz.com"}]},
                                   :cc_recipients=>{:mailbox=>{:name=>"woo wuwoo", :email_address=>"woo.wuwoo@woo.com"}},
                                   :bcc_recipients=>{:mailbox=>{:name=>"fee mcfee", :email_address=>"fee.mcfee@fee.com"}})
          h=c.message_to_hash(m)
          h.should == {
            :message_type=>"email",
            :message_id=>"abc123",
            :sent_at=>sent_at.to_s,
            :in_reply_to=>"foo",
            :references=>["foo", "bar"],
            :from=>{:name=>"foo mcfoo", :email_address=>"foo.mcfoo@foo.com"},
            :sender=>{:name=>"mrs mcmrs", :email_address=>"mrs.mcmrs@foo.com"},
            :to=>[{:name=>"bar mcbar", :email_address=>"bar.mcbar@bar.com"},
                             {:name=>"baz mcbaz", :email_address=>"baz.mcbaz@baz.com"}],
            :cc=>[{:name=>"woo wuwoo", :email_address=>"woo.wuwoo@woo.com"}],
            :bcc=>[{:name=>"fee mcfee", :email_address=>"fee.mcfee@fee.com"}]
          }
        end
      end

      describe "save_messages" do
        def check_saved_msg(c, msg, json_msg)
            h = JSON.parse(json_msg)
            h["connector"].should == c.name
            h["source"].should == c.url
            h["source_id"].should == msg[:item_id][:id]
            h["message_type"].should == "email"
        end

        it "should save a file for each message result" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)

          
          msg1 = {
            :item_id=>{:id=>"abc", :change_key=>"def"},
            "item:DateTimeSent"=>DateTime.now-1,
            "item:DateTimeReceived"=>DateTime.now,
            "item:InReplyTo"=>"foo",
            "message:InternetMessageId"=>"foobar",
            "message:References"=>"barbar",
            "messsage:From"=>"foo@bar.com",
            "message:Sender"=>"foo@bar.com",
            "message:ToRecipients"=>"baz@bar.com",
            "message:CcRecipients"=>"abc@def.com",
            "message:BccRecipients"=>"boss@foo.com"
          }
          msg2 = {
            :item_id=>{:id=>"ghi", :change_key=>"jkl"}}

          msgs = [msg1, msg2]

          mock(c.filestore).write(:complete, "#{MD5.hexdigest('abc')}.json", anything) do |*args|
            check_saved_msg(c, msg1, args.last)
          end
          mock(c.filestore).write(:complete, "#{MD5.hexdigest('ghi')}.json", anything) do |*args|
            check_saved_msg(c, msg2, args.last)
          end

          c.save_messages(msgs)
        end

        it "should log and continue if the extracted message is nil" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          stub(c).is_journal{true}

          msg = Object.new
          msgs=[msg]
          stub(c).extract_journalled_message(msg){nil}
          
          stub(c.log).warn(/no data extracted/)
          dont_allow(c.filestore).write

          c.save_messages(msgs)
        end
      end

      describe "extract_journalled_message" do
        it "should catch any exception extracting the message, log a warning and return nil" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)

          msg = Object.new
          mime_content = Object.new
          stub(msg).[](:mime_content){mime_content}
          mime_msg = Object.new
          stub(Base64).decode64(mime_content){mime_msg}
          e = begin ; raise "bang" ; rescue Exception=>e ; e ; end
          stub(Rfc822Util).extract_journalled_mail(mime_msg){raise e}

          stub(c.log).warn(anything){true}
          
          lambda{
            c.extract_journalled_message(msg).should == nil
          }.should_not raise_error
        end
      end

      describe "delete_messages" do
        it "should HardDelete all messages from a result" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          msgs = Object.new
          mock(msgs).length{1}

          mock(c.client).delete_item(msgs, :delete_type=>:HardDelete)

          c.delete_messages(msgs)
        end
      end
      
    end
  end
end
