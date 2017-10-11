package Reducers;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import com.google.maps.model.DirectionsLeg;
import com.google.maps.model.LatLng;

import MasterWorker.Connection;

public class Reducer {
	Connection master;
	ServerSocket reduceServer;
	ArrayList<Connection> workers;
	ArrayList<DirectionsLeg> directions;
	int requestID = 0;
	LatLng src;
	LatLng dest;
	
	public Reducer(Connection c) {
		this.master = c;
		workers = new ArrayList<Connection>();
		directions = new ArrayList<DirectionsLeg>();
		
		try {
			reduceServer = new ServerSocket(0);
			c.sendData("REDUCER PORT");
			c.sendData(c.socket.getLocalPort());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int workercount = 0;
		if (((String) master.getData()).equals("WORKER COUNT"))
			workercount = (int) master.getData();
		
		if (((String) master.getData()).equals("GetDirections")) {
			src = (LatLng) master.getData();
			dest = (LatLng) master.getData();
		}
		
		
		for (int i = 0; i < workercount; i++) {
			try {
				workers.add(new Connection(reduceServer.accept()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		waitForWorkers();
		waitForACK();
		sendDataToMaster(reduce());
		
	}
	
	public void waitForWorkers() {
		
		for (Connection w : workers) {
			String ans = (String) w.getData();
			if (ans.equals("REQUEST ID")) {
				requestID = (int) w.getData();
			}
			DirectionsLeg l = (DirectionsLeg) w.getData();
			
			if (l != null)
				directions.add(l);
		}
		
	}
	
	public void waitForACK() {
		String ans = (String) master.getData();
		if (ans.equals("START")) {
			
		}
	}
	
	public DirectionsLeg reduce() {
		DirectionsLeg minLeg = null;
		double minDist = -1;
		
		for (DirectionsLeg leg : directions) {
			double dist = (leg.distance.inMeters / 1000) + distance(leg.startLocation, src) + distance(leg.endLocation, dest);
			
			if (minDist == -1 || minDist > dist) {
				minDist = dist;
				minLeg = leg;
			}
		}
		
		return minLeg;
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
	
	public void sendDataToMaster(DirectionsLeg leg) {
		master.sendData(leg);
	}

	public static void main(String[] args) {
		try {
			ServerSocket server = new ServerSocket(2233);
			
			while (true) {
				Socket c = server.accept();
				System.out.println("Connected to MasterNode " + c.getInetAddress().toString());
				new Thread() {
					public void run() {
						
						new Reducer(new Connection(c));
					}
				};
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			
		}

	}

}
