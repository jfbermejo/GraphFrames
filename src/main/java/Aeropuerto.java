public class Aeropuerto {

    private String id;
    private String name;
    private String country;
    private String city;
    private double lat;
    private double lon;

    public Aeropuerto(String id, String name, String country, String city,
                      double lat, double lon){

        super();

        this.id = id;
        this.name = name;
        this.country = country;
        this.city = city;
        this.lat = lat;
        this.lon = lon;
    }

    public String getId() { return id; }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }
}
