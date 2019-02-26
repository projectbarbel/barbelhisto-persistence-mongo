package com.projectbarbel.histo.persistence.mongo;

import java.time.LocalDate;

import org.projectbarbel.histo.DocumentId;
import org.projectbarbel.histo.functions.BarbelPojoSerializer;

import com.googlecode.cqengine.persistence.support.serialization.PersistenceConfig;

@PersistenceConfig(serializer=BarbelPojoSerializer.class, polymorphic=true)
public class WeitereFahrer {

    @DocumentId
    private String id;
    private String geschlecht;
    private LocalDate geburtsdatum;
    protected String getId() {
        return id;
    }
    protected void setId(String id) {
        this.id = id;
    }
    protected String getGeschlecht() {
        return geschlecht;
    }
    protected void setGeschlecht(String geschlecht) {
        this.geschlecht = geschlecht;
    }
    protected LocalDate getGeburtsdatum() {
        return geburtsdatum;
    }
    protected void setGeburtsdatum(LocalDate geburtsdatum) {
        this.geburtsdatum = geburtsdatum;
    }
    
}
