package Mappers;	

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.maps.DirectionsApi;
import com.google.maps.DirectionsApiRequest;
import com.google.maps.GeoApiContext;
import com.google.maps.model.DirectionsLeg;
import com.google.maps.model.DirectionsResult;
import com.google.maps.model.DirectionsRoute;
import com.google.maps.model.LatLng;

import MasterWorker.Connection;
import MasterWorker.MapPoint;

public class Mapper {
	Connection master;
	LatLng dest;
	LatLng src;
	int requestID;
	Connection reducer;
	String reducerIp;
	static String dbFileName = "db.data";
	static ArrayList<DirectionsLeg> db;
	
	public Mapper(Connection c) {
		this.master = c;
		reducerIp = "192.168.10.160";
		
		int reducerport = 0;
		if (((String) master.getData()).equals("REDUCER PORT"))
			reducerport = (int) master.getData();
		
		try {
			reducer = new Connection(new Socket(InetAddress.getByName(reducerIp),reducerport));
			System.out.println("Connected to reducer at " + reducerIp + ":" + reducerport);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		getDataFromMaster();
		
		DirectionsLeg ob = checkDB();
		sendDataToReducer(ob);
		sendMasterDone();
		waitForMaster();
		
	}
	
	private void waitForMaster() {
		String ans = (String) master.getData();
		if (ans.equals("CloseConnection")) {
		}
		
		if (ans.equals("AddDirections")) {
			DirectionsLeg ob = getDirectionsFromAPI();
			master.sendData("API Directions");
			GsonBuilder builder = new GsonBuilder();
			builder.serializeNulls();
			Gson gson = builder.create();
			master.sendData(gson.toJson(ob));
			saveDB();
			waitForMaster();
		}
		
	}

	private DirectionsLeg getDirectionsFromAPI() {
		GeoApiContext context = new GeoApiContext().setApiKey("AIzaSyBLdko72da9xalEOKZybl3MZCfw37wDWug");
		DirectionsApiRequest req = DirectionsApi.newRequest(context);
		req.origin(src);
		req.destination(dest);
		req.alternatives(false);
		try {
			DirectionsResult result = req.await();
			DirectionsRoute route = result.routes[0];
			DirectionsLeg leg = route.legs[0];
			
			System.out.println("Origin: " + leg.startAddress);
			System.out.println("Destination: " + leg.endAddress);
			System.out.println("Distance: " + leg.distance);
			System.out.println("Duration: " + leg.duration);
			System.out.println();
			System.out.println("Directions:");
			
			System.out.println("Got directions for request " + requestID + " from google API");
			
			db.add(leg);
			
			return leg;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private synchronized void saveDB() {
		System.out.println("DB size: " + db.size());
		try {
			GsonBuilder builder = new GsonBuilder();
			builder.serializeNulls();
			Gson gson = builder.create();
			String s = gson.toJson(db, new TypeToken<ArrayList<DirectionsLeg>>() {}.getType());
			FileWriter f = new FileWriter(dbFileName);
			f.write(s);
			f.close();
		} catch (JsonIOException | IOException e) {
			e.printStackTrace();
		}
		
	}

	private void sendMasterDone() {
		master.sendData("DONE");
	}

	private void sendDataToReducer(DirectionsLeg ob) {
		reducer.sendData("REQUEST ID");
		reducer.sendData(requestID);
		reducer.sendData("Directions");
		GsonBuilder builder = new GsonBuilder();
		builder.serializeNulls();
		Gson gson = builder.create();
		reducer.sendData(gson.toJson(ob));
		
	}

	private DirectionsLeg checkDB() {
		double srclat = (double) ((int) (src.lat * 100d)) / 100f;
		double srclong = (double) ((int) (src.lng * 100d)) / 100f;
		double destlat = (double) ((int) (dest.lat * 100d)) / 100f;
		double destlong = (double) ((int) (dest.lng * 100d)) / 100f;
		
		ArrayList<DirectionsLeg> legs = new ArrayList<DirectionsLeg>();
		
		for (DirectionsLeg leg : db) {
			double leg_src_lat = (double) ((int) (leg.startLocation.lat * 100d)) / 100f;
			double leg_src_long = (double) ((int) (leg.startLocation.lng * 100d)) / 100f;
			double leg_dest_lat = (double) ((int) (leg.endLocation.lat * 100d)) / 100f;
			double leg_dest_long = (double) ((int) (leg.endLocation.lng * 100d)) / 100f;
			
			if (srclat == leg_src_lat && srclong == leg_src_long && destlat == leg_dest_lat && destlong == leg_dest_long) {
				legs.add(leg);
			}
		}
		
		if (legs.size() > 0) {
			DirectionsLeg minLeg = null;
			double minDist = -1;
			
			for (DirectionsLeg leg : legs) {
				double dist = (leg.distance.inMeters / 1000) + distance(leg.startLocation, src) + distance(leg.endLocation, dest);
				
				if (minDist == -1 || minDist > dist) {
					minDist = dist;
					minLeg = leg;
				}
			}
			System.out.println("Found directions for request " + requestID + " in DB");
			return minLeg;
		}
		return null;
	}
	
	public double distance(LatLng l1, LatLng l2) {
		double theta = l1.lng - l2.lng;
		double dist = Math.sin(deg2rad(l1.lat)) * Math.sin(deg2rad(l2.lat)) + Math.cos(deg2rad(l1.lat)) * Math.cos(deg2rad(l2.lat)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515 * 1.609344;
		
		return dist;
	}
	
	public double deg2rad(double deg) {
		return deg * Math.PI / 180.0;
	}
	
	public double rad2deg(double rad) {
		return rad * 180.0 / Math.PI;
	}

	public void getDataFromMaster() {
		String ans = (String) master.getData();
		if (ans.equals("CloseConnection")) {
			try {
				Thread.currentThread().join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if (ans.equals("REQUEST ID")) {
			requestID = (int) master.getData();
			ans = (String) master.getData();
		}
		
		if (ans.equals("GetDirections")) {
			src = ((MapPoint) master.getData()).getLagLng();
			System.out.println(src.lat + ", " + src.lng);
			dest = ((MapPoint) master.getData()).getLagLng();
			System.out.println(dest.lat + ", " + dest.lng);
		}
	}
	
	
	public static void main(String args[]) {
		db = readDB();
		if (db == null) {
			db = new ArrayList<DirectionsLeg>();
		}
		
		try {
			ServerSocket server = new ServerSocket(3322);
			
			while (true) {
				Socket c = server.accept();
				System.out.println("Connected to MasterNode " + c.getInetAddress().toString());
				Thread t = new Thread() {
					public void run() {
						new Mapper(new Connection(c));
					}
				};
				t.start();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			
		}
		
	}

	private static ArrayList<DirectionsLeg> readDB() {	
		try {
			GsonBuilder builder = new GsonBuilder();
			builder.serializeNulls();
			Gson gson = builder.create();	
			return gson.fromJson(new JsonReader(new FileReader(dbFileName)), new TypeToken<ArrayList<DirectionsLeg>>(){}.getType());
		} catch (JsonIOException e) {
			e.printStackTrace();
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
}
