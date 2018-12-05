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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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

import com.googlecode.protobuf.format.JsonJacksonFormat;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

@EnableAutoConfiguration
public class Subscriber extends Client implements nEventListener
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);
	
	long startEid;
	String rname = null;
	String selector = null;
	
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
	public void start(String rname, String achannelName, String selector, long startEid)
	{
		LOGGER.info("Starting consumer on " + rname + " for " + achannelName + " using filter " + selector + " at " + startEid);
		this.rname = rname;
		// Process the local REALM RNAME details
		String[] rproperties = new String[4];
		rproperties = parseRealmProperties(rname);
		constructSession(rproperties);
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
				LOGGER.trace("Found defs " + protoDefBytes.length);
				try
				{
					DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(protoDefBytes[0]);
					md = Descriptors.FileDescriptor.buildFrom(set.getFile(0), new  Descriptors.FileDescriptor[] {}).getMessageTypes().get(0);
					LOGGER.trace("Channel has following protodefs: "+ md.getName());
				} catch(Exception e) {
					LOGGER.error("Unable to compile protobuf defitions", e);
				}
			}
			
			// Add this object as a subscribe to the channel with the specified
			// message selector channel and start eid
			LOGGER.trace("Connecting to " + achannelName + " with selector: " + selector);
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
		// display message
		
		
		try
		{
			JsonJacksonFormat formatter = new JsonJacksonFormat();
			ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
			DynamicMessage dm =  DynamicMessage.parseFrom(md, evt.getEventData());
			formatter.print(dm, bos, StandardCharsets.UTF_8);
			LOGGER.info(rname + " - " + evt.getChannelName() + " - " + md.getName() + " - ID:" + evt.getEventID() + " - "  + bos.toString(StandardCharsets.UTF_8.name()));
		} catch(Throwable e) {
			try
			{
				LOGGER.error(rname + " - "  + evt.getChannelName() + " ID:" + evt.getEventID() + " - Unable to parse message", e);
			} catch(Throwable e2) {
				LOGGER.error(rname + " - Unknown error - Unable to print error message with event", e2);
			}
		}
	}
}

