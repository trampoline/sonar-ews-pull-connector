require File.expand_path("../../spec_helper", __FILE__)

module Sonar
  module Connector
    describe EwsPullConnector do

      before do
        setup_valid_config_file
        @base_config = Sonar::Connector::Config.load(valid_config_filename)
      end

      def one_folder_config
        {
          'name'=>'foobarcom-exchange',
          'repeat_delay'=>60,
          'url'=>"https://foo.com/EWS/Exchange.asmx",
          'auth'=>'ntlm',
          'user'=>"foo",
          'password'=>"foopass",
          'distinguished_folders'=>[["inbox", "foo@foo.com"]],
          'batch_size'=>100,
          'delete'=>true
        }
      end

      def two_folder_config
        {
          'name'=>'foobarcom-exchange',
          'repeat_delay'=>60,
          'url'=>"https://foo.com/EWS/Exchange.asmx",
          'auth'=>'ntlm',
          'user'=>"foo",
          'password'=>"foopass",
          'distinguished_folders'=>[["inbox", "foo@foo.com"], "inbox"],
          'batch_size'=>100,
          'delete'=>true
        } 
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

      describe "action" do
        it "should make a Rews find_item request, save, update state, delete" do
          c=Sonar::Connector::EwsPullConnector.new(two_folder_config, @base_config)
          state = {}
          stub(c).state{state}
          
          c.distinguished_folder_ids.each do |fid|
            msg_ids = Object.new
            mock(msg_ids).size{1}
            result = Object.new
            stub(msg_ids).result{result}

            msgs = Object.new

            earlier = DateTime.now-1
            later = DateTime.now

            # state is empty, so timestamp from first message is used
            mock(result).first.mock!.[]("item:DateTimeReceived"){ earlier }
            # called once to check loop and once to update state
            mock(result).last.times(2).mock!.[]("item:DateTimeReceived").times(2){ later }

            mock(fid).find_item(anything){msg_ids}
            mock(fid).get_item(msg_ids, anything){msgs}
            
            mock(c).save_messages(msgs)
            mock(c).delete_messages(fid, msgs)
          end
          c.action
        end

        it "should have a single item:ItemClass Restriction clause if fstate is nil" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          msg_ids = Object.new
          mock(msg_ids).size{1}
          mock(fid).find_item(anything) do |query_opts|
            r = query_opts[:restriction]
            r.should == [:==, "item:ItemClass", "message"] 
            msg_ids
          end

          msgs=Object.new
          mock(fid).get_item(msg_ids, anything){msgs}
          mock(c).save_messages(msgs)
          mock(c).delete_messages(fid, msgs)

          earlier=DateTime.now-1
          later=DateTime.now

          result=Object.new
          stub(msg_ids).result{result}
          # state is empty, so timestamp from first message is used
          mock(result).first.mock!.[]("item:DateTimeReceived"){ earlier }
          mock(result).last.times(2).mock!.[]("item:DateTimeReceived").times(2){later}

          c.action
        end
        
        it "should have a second item:DateTimeReceived Restriction clause if fstate is non-nil" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)

          fid = c.distinguished_folder_ids.first

          state_time = (DateTime.now - 1).to_s
          state = {fid.key => state_time}
          stub(c).state{state}

          msg_ids = Object.new
          mock(msg_ids).size{1}
          mock(fid).find_item(anything) do |query_opts|
            r = query_opts[:restriction]
            r[0].should == :and
              r[1].should == [:==, "item:ItemClass", "message"] 
            r[2].should == [:>=, "item:DateTimeReceived", state_time.to_s]
            msg_ids
          end

          msgs = Object.new
          mock(fid).get_item(msg_ids, anything){msgs}
          mock(c).save_messages(msgs)
          later_time = DateTime.now.to_s
          mock(msg_ids).result.times(2).mock!.last.times(2).mock!.[]("item:DateTimeReceived").times(2){later_time}
          mock(c).delete_messages(fid, msgs)

          c.action
        end

        it "should cycle through messages with identical item:DateTimeReceived, so state is always updated even if 'delete' option is false" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          
          fid = c.distinguished_folder_ids.first

          state_time = (DateTime.now - 1).to_s
          later_time = DateTime.now.to_s
          state = {fid.key => state_time}
          stub(c).state{state}

          msg_ids = Object.new
          mock(msg_ids).size{10}
          msgs = Object.new

          more_msg_ids = Object.new
          mock(more_msg_ids).size{1}
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

          mock(fid).get_item(msg_ids, anything){msgs}
          mock(c).save_messages(msgs)
          mock(c).delete_messages(fid, msgs)
          mock(msg_ids).result.mock!.last.mock!.[]("item:DateTimeReceived"){state_time}

          later_time = DateTime.now
          mock(fid).get_item(more_msg_ids, anything){more_msgs}
          mock(c).save_messages(more_msgs)
          mock(c).delete_messages(fid, more_msgs)
          mock(more_msg_ids).result.mock!.last.mock!.[]("item:DateTimeReceived"){later_time}
          mock(more_msg_ids).result.mock!.last.mock!.[]("item:DateTimeReceived"){later_time}

          c.action
        end
      end

      describe "save_messages" do
        def check_saved_msg(c, msg, json_msg)
            h = JSON.parse(json_msg)
            h["type"].should == "email"
            h["connector"].should == c.name
            h["source"].should == c.url
            h["source_id"].should == JSON.parse(msg[:item_id].to_json)
          
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

          mock(c.filestore).write(:complete, "abc.json", anything) do |*args|
            check_saved_msg(c, msg1, args.last)
          end
          mock(c.filestore).write(:complete, "ghi.json", anything) do |*args|
            check_saved_msg(c, msg2, args.last)
          end

          c.save_messages(msgs)
        end
      end

      describe "delete_messages" do
        it "should HardDelete all messages from a result" do
          c=Sonar::Connector::EwsPullConnector.new(one_folder_config, @base_config)
          fid = c.distinguished_folder_ids.first

          msgs = Object.new
          mock(msgs).length{1}

          mock(fid).delete_item(msgs, :delete_type=>:HardDelete)

          c.delete_messages(fid, msgs)
        end
      end
      
    end
  end
end
