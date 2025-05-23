package edu.uci.ics.tippers.generation.data.WiFi;


public enum DataFiles {

    INFRA ("infrastructure.json") ,
    GROUP ("group.json"),
    USER ("user.json"),
    SO ("semanticObservation.json"),
    SO_FULL ("semanticObservationFull.json"),
    PRESENCE_REAL ("policy.csv"),
    COVERAGE("coverageSensor.txt");


    private final String path;

    private DataFiles(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
