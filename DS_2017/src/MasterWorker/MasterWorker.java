package MasterWorker;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

import com.google.maps.model.DirectionsLeg;
import com.google.maps.model.DirectionsStep;
import com.google.maps.model.LatLng;

// ********************************************************
// Google API key = AIzaSyBLdko72da9xalEOKZybl3MZCfw37wDWug
//********************************************************

public class MasterWorker {
	static MemCache<String, DirectionsLeg> cache;
	ArrayList<Connection> workers;
	Connection reducer;
	String workerIp[];
	String reducerIp;
	LatLng src;
	LatLng dest;
	int requestID;
	
	public MasterWorker() {
		requestID = (new Random().nextInt(2000) + 1);
		
		workerIp = new String[1];
		workerIp[0] = "10.25.40.139";
		//workerIp[1] = "192.168.0.2";
		//workerIp[2] = "192.168.0.3";
		reducerIp = "10.25.40.139";
		workers = new ArrayList<Connection>();
		
		try {
			reducer = new Connection(new Socket(InetAddress.getByName(reducerIp),2233));
			System.out.println("Connected to reducer at " + reducerIp + ":" + 2233);
			
			int reducerport = 0;
			if (((String) reducer.getData()).equals("REDUCER PORT"))
				reducerport = (int) reducer.getData();
			
			for (int i = 0; i < workerIp.length; i++) {
				workers.add(new Connection(new Socket(InetAddress.getByName(workerIp[i]),3322)));
				System.out.println("Connected to worker at " + workerIp[i] + ":" + 3322);
			}
			
			reducer.sendData("WORKER COUNT");
			reducer.sendData(workers.size());
			
			for (Connection w : workers) {
				w.sendData("REDUCER PORT");
				w.sendData(reducerport);
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		getUserInput();
		
		DirectionsLeg ob = checkCache();
		
		if (ob == null) {
			for (Connection c : workers) {
				c.sendData("REQUEST ID");
				c.sendData(requestID);
				c.sendData("GetDirections");
				c.sendData(src);
				c.sendData(dest);
			}
			
			reducer.sendData("GetDirections");
			reducer.sendData(src);
			reducer.sendData(dest);
			
			waitForMappers();
			sendAckToReducer();
			ob = waitForReducer();
		}
		
		if (ob == null) {
			MessageDigest md5;
			try {
				md5 = MessageDigest.getInstance("MD5");
				md5.reset();
				md5.update(("" + src.lat + src.lng + dest.lat + dest.lng).getBytes());
				BigInteger direDigest = new BigInteger(1, md5.digest());
				
				direDigest = direDigest.mod(BigInteger.valueOf(workerIp.length));
				
				int workerIndex = direDigest.intValue();
				Connection c = workers.get(workerIndex);
				c.sendData("AddDirections");
				
				if(((String) c.getData()).equals("API Directions")) {
					ob = (DirectionsLeg) c.getData();
				}
				
				
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		for (Connection c : workers) {
			c.sendData("CloseConnection");
		}
		
		if (ob == null) {
			System.out.println("Can't find directions for the specified location.");
		}else {
			printDirections(ob);
			addToCache(ob);
		}
		
		
	}
	
	private void addToCache(DirectionsLeg ob) {
		
		double srclat = (double) ((int) (src.lat * 100d)) / 100f;
		double srclong = (double) ((int) (src.lng * 100d)) / 100f;
		double destlat = (double) ((int) (dest.lat * 100d)) / 100f;
		double destlong = (double) ((int) (dest.lng * 100d)) / 100f;
		cache.put((new LatLng(srclat, srclong)).toString() + ":" + (new LatLng(destlat, destlong)).toString(), ob);
	}

	public void printDirections(DirectionsLeg leg) {
		System.out.println("Origin: " + leg.startAddress);
		System.out.println("Destination: " + leg.endAddress);
		System.out.println("Distance: " + leg.distance);
		System.out.println("Duration: " + leg.duration);
		System.out.println();
		System.out.println("Directions:");
		
		for (DirectionsStep step : leg.steps) {
			System.out.println(step.htmlInstructions.replaceAll("\\<.*?>",""));
		}
	}
	
	public void getUserInput() {
		Scanner input = new Scanner(System.in);
		System.out.println("Enter Source Latitude");
		double srcLat = input.nextDouble();
		System.out.println("Enter Source Longtitude");
		double srcLong = input.nextDouble();
		System.out.println("Enter Destination Latitude");
		double destLat = input.nextDouble();
		System.out.println("Enter Destination Longtitude");
		double destLong = input.nextDouble();
		
		input.close();
		
		src = new LatLng(srcLat, srcLong);
		dest = new LatLng(destLat, destLong);
	}
	
	public DirectionsLeg checkCache() {
		double srclat = (double) ((int) (src.lat * 100d)) / 100f;
		double srclong = (double) ((int) (src.lng * 100d)) / 100f;
		double destlat = (double) ((int) (dest.lat * 100d)) / 100f;
		double destlong = (double) ((int) (dest.lng * 100d)) / 100f;
		return cache.get((new LatLng(srclat, srclong)).toString() + ":" + (new LatLng(destlat, destlong)).toString());
	}
	
	public void waitForMappers() {
		System.out.println("Waiting for Mappers...");
		
		for (int i = 0; i < workers.size(); i++) {
			Connection c = workers.get(i);
			String s = (String) c.getData();
			if (s.equals("DONE")) {
				System.out.println("Worker " + i + " is done.");
			}
		}
	}
	
	public void sendAckToReducer() {
		System.out.println("\nSending ACK to reducer.");
		reducer.sendData("START");
	}
	
	public DirectionsLeg waitForReducer() {
		System.out.println("Waiting for Reducer...");
		
		return (DirectionsLeg) reducer.getData();
	}
	
	public static void main(String[] args) {
		cache = new MemCache<String, DirectionsLeg>(100);
		
		new MasterWorker();
	}
	
	
}
