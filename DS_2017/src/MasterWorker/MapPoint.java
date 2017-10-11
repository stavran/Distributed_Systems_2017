package MasterWorker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.google.maps.model.LatLng;

public class MapPoint implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private transient LatLng location;
	
	public MapPoint(LatLng l) {
		location = l;
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeDouble(location.lat);
        out.writeDouble(location.lng);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        location = new LatLng(in.readDouble(), in.readDouble());
    }
    
    public LatLng getLagLng() {
    	return location;
    }
    
}
