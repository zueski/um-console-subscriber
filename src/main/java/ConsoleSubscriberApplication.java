package com.amway.integration.um.console;


import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SpringBootApplication
@EnableAutoConfiguration
public class ConsoleSubscriberApplication implements ApplicationRunner
{
	private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);

	@Override
	public void run(ApplicationArguments args) throws Exception 
	{
		LOGGER.info("Application started with command-line arguments: {}", Arrays.toString(args.getSourceArgs()));
		
		// Check the local realm details
		List<String> rnames = args.getOptionValues("rname");
		if(rnames ==  null || rnames.size() < 1)
		{
			Usage();
			System.exit(1);
		}
		// start a consumer for each channel
		List<String> channels = args.getOptionValues("channel");
		if(channels == null || channels.size() < 1)
		{
			Usage();
			System.exit(1);
		}
		List<String> selectors = args.getOptionValues("selector");
		if(selectors == null)
		{	selectors = new ArrayList<String>(); }
		List<String> starts = args.getOptionValues("start");
		if(starts == null)
		{	starts = new ArrayList<String>(); }
		
		for(int i = 0; i < channels.size(); i++)
		{
			String rname = (i < rnames.size()) ? rnames.get(i) : rnames.get(0);
			String channel = channels.get(i);
			String selector = (selectors.size() > i) ? selectors.get(i) : null;
			Long start = -1L;
			if(i < starts.size())
			{	try { start = Long.parseLong(starts.get(i)); } catch(Exception e) { } }
			
			LOGGER.info("Creating consumer on " + rname + " for " + channel + " using filter " + selector + " at " + start);
			
			Properties props = new Properties();
			props.put("rname", rname);
			props.put("channel", channel);
			// Create an instance for this class
			Subscriber subscriber = new Subscriber(props);
			// Subscribe to the channel specified
			subscriber.start(rname, channel, selector, start);
		}
	}
	
	public static void main(String[] args) 
	{
		SpringApplication.run(ConsoleSubscriberApplication.class, args);
	}
	
	/**
	 * Prints the usage message for this class
	 */
	private static void Usage() 
	{
		LOGGER.error("Usage ...\n");
		LOGGER.error("  call with each setting as a key:value pair on commandline, e.g.:\n");
		LOGGER.error("    --rname=nhp://uslx416:9000 --channel=sampleChannel\n");
		LOGGER.error("----------- Required Arguments> -----------\n");
		LOGGER.error("channel:  Channel name parameter for the channel to subscribe to");
		LOGGER.error("\n----------- Optional Arguments -----------\n");
		LOGGER.error("start:  The Event ID to start subscribing from");
		LOGGER.error("selector:  The event filter string to use\n");
		Subscriber.UsageEnv();
	}
}
