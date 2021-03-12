package com.ipiecoles.batch.processor;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.model.Commune;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.annotation.BeforeProcess;
import org.springframework.batch.core.annotation.OnProcessError;
import org.springframework.batch.item.ItemProcessor;

public class CommuneProcessor implements ItemProcessor<CommuneDto, Commune> {
    @Override
    public Commune process(CommuneDto item) throws Exception {
        Commune commune = new Commune();
        validateCommuneCSV(item);
        commune.setCodeInsee(item.getCodeInsee());
        commune.setCodePostal(item.getCodePostal());
        String nomCommune = (WordUtils.capitalizeFully(item.getNom()));
        nomCommune = nomCommune.replaceAll("^L ", "L'");
        nomCommune = nomCommune.replaceAll(" L ", " L'");
        nomCommune = nomCommune.replaceAll("^D ", "D'");
        nomCommune = nomCommune.replaceAll(" D ", " D'");
        nomCommune = nomCommune.replaceAll("^St ", "Saint ");
        nomCommune = nomCommune.replaceAll(" St ", " Saint ");
        nomCommune = nomCommune.replaceAll("^Ste ", "Sainte ");
        nomCommune = nomCommune.replaceAll(" Sainte ", " Sainte ");
        commune.setNom(nomCommune);
        String[] gps = item.getGps().split(",");
        if(gps.length == 2){
            commune.setLatitude(Double.valueOf(gps[0]));
            commune.setLongitude(Double.valueOf(gps[1]));
        }
        return commune;

    }

    private void validateCommuneCSV(CommuneDto item) throws CommuneCSVException {
        //Contrôler Code INSEE 5 chiffres
        if(item.getCodeInsee() != null && !item.getCodeInsee().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Insee ne contient pas 5 chiffres");
        }
        //Contrôler Code postal 5 chiffres
        if(item.getCodePostal() != null && !item.getCodePostal().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Postal ne contient pas 5 chiffres");
        }
        //Contrôler nom de la communes lettres en majuscules, espaces, tirets, et apostrophes
        if(item.getNom() != null && !item.getNom().matches("^[A-Z-' ]+$")){
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
        //Contrôler les coordonnées GPS
        if(item.getGps() != null && !item.getGps().matches("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")){
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
    }

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @BeforeProcess
    public void beforeProcess(CommuneDto input){
        logger.info("Before Process => " + input.toString());
    }

    @AfterProcess
    public void afterProcess(CommuneDto input, Commune output) {
        logger.info("After Process => " + input.toString() + " => " + output.toString());
    }

    @OnProcessError
    public void onProcessError(CommuneDto input, Exception ex) {
        logger.error("Error Process => " + input.toString() + " => " + ex.getMessage());
    }
}
