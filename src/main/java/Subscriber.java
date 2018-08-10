package com.amway.integration.um.console;

import com.pcbsys.nirvana.client.nBaseClientException;
import com.pcbsys.nirvana.client.nChannel;
import com.pcbsys.nirvana.client.nChannelAlreadySubscribedException;
import com.pcbsys.nirvana.client.nChannelAttributes;
import com.pcbsys.nirvana.client.nChannelNotFoundException;
import com.pcbsys.nirvana.client.nConsumeEvent;
import com.pcbsys.nirvana.client.nProtobufEvent;
import com.pcbsys.nirvana.client.nEventListener;
import com.pcbsys.nirvana.client.nEventProperties;
import com.pcbsys.nirvana.client.nRequestTimedOutException;
import com.pcbsys.nirvana.client.nSecurityException;
import com.pcbsys.nirvana.client.nSelectorParserException;
import com.pcbsys.nirvana.client.nSelectorParserException;
import com.pcbsys.nirvana.client.nSessionFactory;
import com.pcbsys.nirvana.client.nSessionNotConnectedException;
import com.pcbsys.nirvana.client.nUnexpectedResponseException;
import com.pcbsys.nirvana.client.nUnknownRemoteRealmException;

import java.io.BufferedInputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import com.googlecode.protobuf.format.JsonFormat;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

@SpringBootApplication
@EnableAutoConfiguration
public class Subscriber extends Client implements nEventListener, ApplicationRunner
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);
	
	static long startEid;
	static String selector = null;
	
	private long lastEID = 0;
	private long startTime = 0;
	private long byteCount = 0;

	private int count = -1;
	private int totalMsgs = 0;
	
	private nChannel myChannel;
	
	//private DescriptorProtos.FileDescriptorSet descriptorSet = null;
	private Descriptors.Descriptor md = null;
	
	public Subscriber(Properties props)
	{
		super(props); 
		processEnvironmentVariables();
	}
	
	/**
	 * This method demonstrates the Nirvana API calls necessary to subscribe to
	 * a channel. It is called after all command line arguments have been
	 * received and validated
	 * 
	 * @param realmDetails
	 *            a String[] containing the possible RNAME values
	 * @param achannelName
	 *            the channel name to create
	 * @param selector
	 *            the subscription selector filter
	 * @param startEid
	 *            the eid to start subscribing from
	 * @param loglvl
	 *            the specified log level
	 * @param repCount
	 *            the specified report count
	 */
	private void doit(String[] realmDetails, String achannelName, String selector, long startEid)
	{
		constructSession(realmDetails);
		// Subscribes to the specified channel
		try 
		{
			// Create a channel attributes object
			nChannelAttributes nca = new nChannelAttributes();
			nca.setName(achannelName);
			// Obtain the channel reference
			myChannel = mySession.findChannel(nca);
			// if the latest event has been implied (by specifying -1)
			if(startEid == -1) 
			{
				// Get the last eid on the channel and reset the start eid with
				// that value
				startEid = myChannel.getLastEID();
			}
			
			
			byte[][] protoDefBytes = myChannel.getChannelAttributes().getProtobufDescriptorSets();
			if(protoDefBytes != null && protoDefBytes.length > 0)
			{
				LOGGER.info("Found defs " + protoDefBytes.length);
				try
				{
					DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(protoDefBytes[0]);
					md = Descriptors.FileDescriptor.buildFrom(set.getFile(0), new  Descriptors.FileDescriptor[] {}).getMessageTypes().get(0);
					LOGGER.info("Channel has following protodefs: "+ md.getName());
				} catch(Exception e) {
					LOGGER.error("Unable to compile protobuf defitions", e);
				}
			}
			
			// Add this object as a subscribe to the channel with the specified
			// message selector channel and start eid
			myChannel.addSubscriber(this, selector, startEid);
		} catch (nChannelNotFoundException cnfe) {
			LOGGER.error("The channel specified could not be found.", cnfe);
			System.exit(1);
		} catch (nSecurityException se) {
			LOGGER.error("Insufficient permissions for the requested operation.", se);
			System.exit(1);
		} catch (nSessionNotConnectedException snce) {
			LOGGER.error("The session object used is not physically connected to the Nirvana Realm.", snce);
			System.exit(1);
		} catch (nUnexpectedResponseException ure) {
			LOGGER.error("The Nirvana REALM has returned an unexpected response.", ure);
			System.exit(1);
		} catch (nUnknownRemoteRealmException urre) {
			LOGGER.error("The channel specified resided in a remote realm which could not be found.", urre);
			System.exit(1);
		} catch (nRequestTimedOutException rtoe) {
			LOGGER.error("The requested operation has timed out waiting for a response from the REALM.", rtoe);
			System.exit(1);
		} catch (nChannelAlreadySubscribedException chase) {
			LOGGER.error("You are already subscribed to this channel.", chase);
			System.exit(1);
		} catch (nSelectorParserException spe) {
			LOGGER.error("An error occured while parsing the selector filter specified.", spe);
			System.exit(1);
		} catch (nBaseClientException nbce) {
			LOGGER.error("An error occured while creating the Channel Attributes object.", nbce);
			System.exit(1);
		}
		
		//try 
		//{
		//	//this.wait();
		//	//nSessionFactory.close(mySession);
		//} catch (Exception ex) { }
		// Close any other sessions within this JVM so that we can exit
		//nSessionFactory.shutdown();
	}
	
	/**
	 * A callback is received by the API to this method each time an event is
	 * received from the nirvana channel. Be carefull not to spend too much time
	 * processing the message inside this method, as until it exits the next
	 * message can not be pushed.
	 * 
	 * @param evt
	 *            An nConsumeEvent object containing the message received from
	 *            the channel
	 */
	public void go(nConsumeEvent evt) 
	{
		// If this is the first message we receive
		if(count == -1) 
		{	// Get a timestamp to be used for message rate calculations
			startTime = System.currentTimeMillis();
			count = 0;
		}
		// display message
		try
		{
			DynamicMessage dm =  DynamicMessage.parseFrom(md, evt.getEventData());
			LOGGER.error(JsonFormat.printToString(dm));
		} catch(Exception e) {
			LOGGER.error("Unable to parse message", e);
		}
		// Increment he counter
		count++;
		totalMsgs++;
	}
	
	@Override
	public void run(ApplicationArguments args) throws Exception 
	{
		LOGGER.info("Application started with command-line arguments: {}", Arrays.toString(args.getSourceArgs()));
		// Process command line arguments
		Properties props = new Properties();
		for(String name : args.getOptionNames())
		{
			List<String> opts = args.getOptionValues(name);
			props.put(name, opts.get(0));
		}
		// Create an instance for this class
		Subscriber subscriber = new Subscriber(props);
		// Check the channel name specified
		String channelName = null;
		if(subscriber.getProperty("channel") != null)
		{
			channelName = subscriber.getProperty("channel");
		} else {
			Usage();
			System.exit(1);
		}
		startEid = -1; // Default value (points to last event in the channel +  1)
		// Check to see if a start EID value has been specified
		if(subscriber.getProperty("start") != null) 
		{
			try 
			{
				startEid = Integer.parseInt(subscriber.getProperty("START"));
			} catch (Exception num) { } // Ignore and use the defaults
		}
		
		// Check for a selector message filter value
		selector = System.getProperty("selector");

		// Check the local realm details
		int idx = 0;
		String RNAME = null;
		if(subscriber.getProperty("rname") != null)
		{
			RNAME = subscriber.getProperty("rname");
		} else {
			Usage();
			System.exit(1);
		}

		// Process the local REALM RNAME details
		String[] rproperties = new String[4];
		rproperties = parseRealmProperties(RNAME);

		// Subscribe to the channel specified
		subscriber.doit(rproperties, channelName, selector, startEid);
	}
	
	public static void main(String[] args) 
	{
		SpringApplication.run(Subscriber.class, args);
	}
	
	/**
	 * Prints the usage message for this class
	 */
	private static void Usage() 
	{
		LOGGER.error("Usage ...\n");
		LOGGER.error("  call with each setting as a key:value pair on commandline, e.g.:\n");
		LOGGER.error("    runSubscriber CHANNAME:sampleChannel SIZE:100 RNAME:nhp://uslxcd001619:9001 DEBUG:3\n");
		LOGGER.error("----------- Required Arguments> -----------\n");
		LOGGER.error("CHANNAME:  Channel name parameter for the channel to subscribe to");
		LOGGER.error("\n----------- Optional Arguments -----------\n");
		LOGGER.error("START:  The Event ID to start subscribing from");
		LOGGER.error("SELECTOR:  The event filter string to use\n");
		UsageEnv();
	}

} // End of subscriber Class
