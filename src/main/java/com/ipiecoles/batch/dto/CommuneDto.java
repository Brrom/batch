package com.ipiecoles.batch.dto;

public class CommuneDto {
    private String codeInsee;
    private String nom;
    private String codePostal;
    private String line;
    private String acheminement;
    private String gps;

    public String getCodeInsee() {
        return codeInsee;
    }

    public CommuneDto setCodeInsee(String codeInsee) {
        this.codeInsee = codeInsee;
        return this;
    }

    public String getNom() {
        return nom;
    }

    public CommuneDto setNom(String nom) {
        this.nom = nom;
        return this;
    }

    public String getCodePostal() {
        return codePostal;
    }

    public CommuneDto setCodePostal(String codePostal) {
        this.codePostal = codePostal;
        return this;
    }

    public String getLine() {
        return line;
    }

    public CommuneDto setLine(String line) {
        this.line = line;
        return this;
    }

    public String getAcheminement() {
        return acheminement;
    }

    public CommuneDto setAcheminement(String acheminement) {
        this.acheminement = acheminement;
        return this;
    }

    public String getGps() {
        return gps;
    }

    public CommuneDto setGps(String gps) {
        this.gps = gps;
        return this;
    }
}
