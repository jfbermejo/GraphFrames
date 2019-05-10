public class Ruta {

    private String src;
    private String dst;
    private String type;
    private int distance;

    public Ruta(String src, String dst, String type, int distance){
        super();

        this.src = src;
        this.dst = dst;
        this.type = type;
        this.distance = distance;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }
}
