/*
*
*   Copyright (c) 1999 - 2011 my-Channels Ltd
*   Copyright (c) 2012 - 2016 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
*
*   Use, reproduction, transfer, publication or disclosure is prohibited except as specifically provided for in your License Agreement with Software AG.
*
*/
package com.amway.integration.um.console;

import com.pcbsys.foundation.utils.fEnvironment;
import com.pcbsys.nirvana.client.nAsyncExceptionListener;
import com.pcbsys.nirvana.client.nBaseClientException;
import com.pcbsys.nirvana.client.nDataStream;
import com.pcbsys.nirvana.client.nDataStreamListener;
import com.pcbsys.nirvana.client.nEventAttributes;
import com.pcbsys.nirvana.client.nEventProperties;
import com.pcbsys.nirvana.client.nEventPropertiesIterator;
import com.pcbsys.nirvana.client.nIllegalArgumentException;
import com.pcbsys.nirvana.client.nRealmUnreachableException;
import com.pcbsys.nirvana.client.nReconnectHandler;
import com.pcbsys.nirvana.client.nSecurityException;
import com.pcbsys.nirvana.client.nSession;
import com.pcbsys.nirvana.client.nSessionAlreadyInitialisedException;
import com.pcbsys.nirvana.client.nSessionAttributes;
import com.pcbsys.nirvana.client.nSessionFactory;
import com.pcbsys.nirvana.client.nSessionNotConnectedException;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
* Base class that contains standard functionality for a nirvana sample app
*/
public abstract class Client implements nReconnectHandler, nAsyncExceptionListener
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
	
	private String myLastSessionID;
	private Properties props;
	protected nSession mySession = null;
	protected nSessionAttributes nsa = null;
	protected nDataStream myStream = null;
	private static final String DEFAULT_USERNAME = "um-console-subscriber-" + System.getProperty("user.name");
	
	public Client()
	{	this.props = System.getProperties(); }
	
	public Client(Properties props)
	{	this.props = props; }
	
	protected void processEnvironmentVariable(String variable)
	{
		String laxVAR = props.getProperty("lax.nl.env."+variable);
		if(laxVAR == null)
		{
			laxVAR = System.getenv(variable);
			if(laxVAR != null)
			{	props.put(variable,laxVAR); }
		}
	}

	protected void processEnvironmentVariables()
	{
		//Process Environment Variables
		processEnvironmentVariable("rname");
		processEnvironmentVariable("loglevel");
		processEnvironmentVariable("hproxy");
		processEnvironmentVariable("hauth");
		processEnvironmentVariable("ckeystore");
		processEnvironmentVariable("ckeystorepasswd");
		processEnvironmentVariable("cakeystore");
		processEnvironmentVariable("cakeystorepasswd");
		processEnvironmentVariable("um_username");
		// Install any proxy server settings
		fEnvironment.setProxyEnvironments();
		// Install any ssl settings
		fEnvironment.setSSLEnvironments();
	}
	
	public String getProperty(String key)
	{	return props.getProperty(key); }

	/**
	* This method processes a string consisting of one or more comma separated
	* RNAME values and splits them into a a String[]
	*
	* @param realmdetails The RNAME of the Nirvana realm
	*/
	protected static String[] parseRealmProperties(String realmdetails)
	{
		String[] rproperties = new String[4];
		int idx = 0;
		String RNAME=null;
		StringTokenizer st=new StringTokenizer(realmdetails,",");
		while(st.hasMoreTokens())
		{
			String someRNAME=(String)st.nextToken();
			rproperties[idx] = someRNAME;
			idx++;
		}
		//Trim the array
		String[] rpropertiesTrimmed=new String[idx];
		System.arraycopy(rproperties,0,rpropertiesTrimmed,0,idx);
		return rpropertiesTrimmed;
	}

	/**
	* This method demonstrates the Nirvana API calls necessary to construct a
	* nirvana session
	*
	* @param realmDetails a String[] containing the possible RNAME values
	*/
	protected void constructSession( String[] realmDetails) 
	{	constructSession(realmDetails, null); }

	/**
	* This method demonstrates the Nirvana API calls necessary to construct a
	* nirvana session
	*
	* @param realmDetails a String[] containing the possible RNAME values
	*/
	protected void constructSession( String[] realmDetails, nDataStreamListener listener)
	{
		//Create a realm session attributes object from the array of strings
		try 
		{
			nsa = new nSessionAttributes(realmDetails, 2);
			nsa.setFollowTheMaster(true);
			nsa.setDisconnectOnClusterFailure(false);
			nsa.setName(getClass().getSimpleName());
		} catch (Exception ex) {
			LOGGER.error("Error creating Session Attributes. Please check your RNAME", ex);
			System.exit(1);
		}
		String USERNAME = props.getProperty("um_username", DEFAULT_USERNAME);
		//Add this class as an asynchronous exception listener
		try 
		{
			//Create a session object from the session attributes object, passing this
			//as a reconnect handler class (optional). This will ensure that the reconnection
			// methods will get called by the API.
			mySession = nSessionFactory.create(nsa, this, USERNAME);
			mySession.addAsyncExceptionListener(this);
			mySession.enableThreading(4);
		} catch (nIllegalArgumentException ex) { }
		//Initialise the Nirvana session. This physically opens the connection to the
		//Nirvana realm, using the specified protocols. If multiple interfaces are supported
		//these will be attempted in weight order (SSL, HTTPS, socket, HTTP).
		try
		{
			if(listener == null) {
			mySession.init();
			} else {
			myStream = mySession.init(false, listener);
			}
			myLastSessionID=mySession.getId();
		} catch(nSecurityException sec) {
			LOGGER.error("The current user is not authorised to connect to the specified Realm Server",sec );
			System.exit(1);
		} catch (nRealmUnreachableException rue) {
			LOGGER.error("The Nirvana Realm specified by the RNAME value is not reachable.", rue);
			System.exit(1);
		} catch (nSessionNotConnectedException snce) {
			LOGGER.error("The session object used is not physically connected to the Nirvana Realm.", snce);
			System.exit(1);
		} catch (nSessionAlreadyInitialisedException ex) {
			LOGGER.error("The session object has already been initialised.", ex);
			ex.printStackTrace();
			System.exit(1);
		}
	}
	/**
	* A callback is received by the API to this method to notify the user of a disconnection
	* from the realm. The method is enforced by the nReconnectHandler interface but is normally optional.
	* It gives the user a chance to log the disconnection or do something else about it.
	*
	* @param anSession The Nirvana session being disconnected
	*/
	public void disconnected(nSession anSession)
	{
		try
		{
			LOGGER.info("You have been disconnected from "+myLastSessionID);
		} catch (Exception ex) {
			LOGGER.error("Error while disconnecting ", ex);
		}
	}

	/**
	* A callback is received by the API to this method to notify the user of a successful reconnection
	* to the realm. The method is enforced by the nReconnectHandler interface but is normally optional.
	* It gives the user a chance to log the reconnection or do something else about it.
	*
	* @param anSession The Nirvana session being reconnected
	*/
	public void reconnected(nSession anSession)
	{
		try
		{
			myLastSessionID=mySession.getId();
			LOGGER.info("You have been Reconnected to "+myLastSessionID);
		} catch (Exception ex) {
			LOGGER.error("Error while reconnecting ", ex);
		}
	}

	/**
	* A callback is received by the API to this method to notify the user that the API is about
	* to attempt reconnecting to the realm. The method is enforced by the nReconnectHandler
	* interface but is normally optional. It allows the user to decide whether further
	* attempts are required or not, whether custom delays should be enforced etc.
	*
	* @param anSession The Nirvana session that will be used to reconnect
	*/
	public boolean tryAgain(nSession anSession)
	{
		try 
		{
			LOGGER.info("Attempting to reconnect to "+props.get("rname"));
		} catch (Exception ex) {
			LOGGER.error("Error while trying to reconnect ", ex);
		}
		return true;
	}

	/**
	* A callback is received by the API to this method to notify the user that the an
	* asynchronous exception (in a thread different than the current one) has occured.
	*
	* @param ex The asynchronous exception that was thrown
	*/
	public void handleException(nBaseClientException ex)
	{
		LOGGER.error("An Asynchronous Exception was received from the Nirvana realm.", ex);
	}

	static protected Properties processArgs(String[] args) 
	{
		if(args != null && args.length > 0)
		{
			Properties props = new Properties();
			for(int i = 0; i < args.length; i++)
			{
				int offset = args[i].indexOf(":");
				if(offset > -1)
				{	props.put(args[i].substring(0,offset), args[i].substring(offset+1,args[i].length())); }
			}
			return props;
		} else {
			return System.getProperties();
		}
	}
	
	public static void UsageEnv() 
	{
		System.err.println("----------- Connecton Settings ----------- \n");
		System.err.println("rname:  One or more RNAME entries in the form protocol://host:port" );
		System.err.println("   protocol - Can be one of nsp, nhp, nsps, or nhps, where:" );
		System.err.println("   nsp - Specifies Nirvana Socket Protocol (nsp)" );
		System.err.println("   nhp - Specifies Nirvana HTTP Protocol (nhp)" );
		System.err.println("   nsps - Specifies Nirvana Socket Protocol Secure (nsps), i.e. using SSL/TLS" );
		System.err.println("   nhps - Specifies Nirvana HTTP Protocol Secure (nhps), i.e. using SSL/TLS" );
		System.err.println("   port - The port number of the server" );
		System.err.println(" (Hint: - For multiple RNAME entries, use comma separated values which will be attempted in connection weight order)" );
		System.err.println("loglevel:  This determines how much information the nirvana api will output 0 = verbose 7 = quiet (not the client)" );
		System.err.println("ckeystore:  If using SSL, the location of the keystore containing the client cert");
		System.err.println("ckeystorepasswd:  If using SSL, the password for the keystore containing the client cert");
		System.err.println("cakeystore:  If using SSL, the location of the ca truststore");
		System.err.println("cakeystorepasswd:  If using SSL, the password for the ca truststore");
		System.err.println("hproxy:  HTTP Proxy details in the form proxyhost:proxyport, where:" );
		System.err.println("   proxyhost - The HTTP proxy host" );
		System.err.println("   proxyport - The HTTP proxy port" );
		System.err.println("hauth:  HTTP Proxy authentication details in the form user:pass, where:" );
		System.err.println("   user - The HTTP proxy authentication username" );
		System.err.println("   pass - The HTTP proxy authentication password" );
	}
	
	// methods to debug events
	
	protected void displayEventProperties(nEventProperties prop)
	{
		LOGGER.info("----------------------------------------------------------------");
		displayEventProperties(prop, 0);
		LOGGER.info("----------------------------------------------------------------");
	}

	protected void displayEventProperties(nEventProperties prop, int level)
	{
		String tab = "";
		for(int x=0;x<level;x++)
		{	tab += "\t"; }
		LOGGER.info(tab+"Event Prop : ");
		List<String> list = Collections.<String>list(prop.getKeysAsStrings());
		Collections.sort(list,new Comparator<String>() { @Override public int compare(String o1, String o2) { return o1.compareTo(o2); } });
			//nEventPropertiesIterator keys = prop.getKeyIterator();
		Iterator keys = list.iterator();
		while(keys.hasNext())
		{
			Object key = keys.next();
			Object value = prop.get(key.toString());
			if(value instanceof nEventProperties)
			{
				nEventProperties pvalue=(nEventProperties)value;
				LOGGER.info(tab+"["+key +"(event prop)]:");
				displayEventProperties(pvalue, level+1);
			} else if(value instanceof nEventProperties[]) {
				nEventProperties[] pvalue=(nEventProperties[])value;
				LOGGER.info(tab+"["+key +"(event prop[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	displayEventProperties(pvalue[x], level+1); }
			} else if(value instanceof String[]) {
				String[] pvalue = (String[])value;
				LOGGER.info(tab+"["+key +"(String[])]:");
				for(int x = 0; x < pvalue.length; x++) 
				{	LOGGER.info("   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof long[]) {
				long[] pvalue = (long[])value;
				LOGGER.info(tab+"["+key +"(long[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info("   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof int[]) {
				int[] pvalue = (int[])value;
				LOGGER.info(tab+"["+key +"(int[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info(tab+"   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof byte[]) {
				byte[] pvalue = (byte[])value;
				LOGGER.info(tab+"["+key +"(byte[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info(tab+"   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof boolean[]) {
				boolean[] pvalue = (boolean[])value;
				LOGGER.info(tab+"["+key +"(boolean[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info(tab+"   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof double[]) {
				double[] pvalue = (double[])value;
				LOGGER.info(tab+"["+key +"(double[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info(tab+"   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof short[]) {
				short[] pvalue = (short[])value;
				LOGGER.info(tab+"["+key +"(short[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info(tab+"   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else if(value instanceof char[]) {
				char[] pvalue = (char[])value;
				LOGGER.info(tab+"["+key +"(char[])]:");
				for(int x = 0; x < pvalue.length; x++)
				{	LOGGER.info(tab+"   ["+key+"]:["+x+"]="+pvalue[x]); }
			} else {
				LOGGER.info(tab+key.toString()+" = "+value.toString());
			}
		}
  }

	protected void displayEventAttributes(nEventAttributes attributes)
	{
		LOGGER.info("Merge Allowed : "+attributes.allowMerging());
		LOGGER.info("Message Type  : "+attributes.getMessageType());
		LOGGER.info("Delivery mode : "+attributes.getDeliveryMode());
		LOGGER.info("Priority      : "+attributes.getDeliveryMode());
		if(attributes.getApplicationId() != null)
		{	LOGGER.info("Application Id : "+new String(attributes.getApplicationId())); }
		if(attributes.getCorrelationId() != null)
		{	LOGGER.info("Correlation Id : "+new String(attributes.getCorrelationId())); }
		if(attributes.getMessageId() != null)
		{	LOGGER.info("Message Id     : "+new String(attributes.getMessageId())); }
		if(attributes.getPublisherHost() != null)
		{	LOGGER.info("Published from : "+new String(attributes.getPublisherHost())); }
		if(attributes.getPublisherName() != null)
		{	LOGGER.info("Published by   : "+new String(attributes.getPublisherName())); }
		if(attributes.getTimestamp() != 0)
		{	LOGGER.info("Published on   : "+new Date(attributes.getTimestamp()).toString()); }
		if(attributes.getReplyToName() != null)
		{	LOGGER.info("Reply To       : "+new String(attributes.getReplyToName())); }
		if(attributes.getExpiration() != 0)
		{	LOGGER.info("Expires on     : "+attributes.getExpiration()); }
		if(attributes.isRedelivered())
		{	LOGGER.info("Redelivered event : "+attributes.getRedeliveredCount()); }
	}

} // End of subscriber Class

