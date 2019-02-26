package com.projectbarbel.histo.persistence.mongo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import org.projectbarbel.histo.DocumentId;
import org.projectbarbel.histo.functions.BarbelPojoSerializer;

import com.googlecode.cqengine.persistence.support.serialization.PersistenceConfig;

@PersistenceConfig(serializer=BarbelPojoSerializer.class, polymorphic=true)
public class KfzVersicherungsVertrag {

    @DocumentId
    private String vnr;
    private List<WeitereFahrer> fahrer;
    private LocalDate faelligkeit;
    private int lz;
    private BigDecimal rabatt;
    private String vermittler;
    private BigDecimal zuschlag;
    private BigDecimal rabattAbsolut;
    private LocalDate urspruenglicherBeginn;
    private BigDecimal intervallBeitragGesamt;
    private LocalDate vertragsBeginn;
    private LocalDate vertragsEnde;
    private LocalDate gueltigVon;
    private String geschaeftsvorfall;
    protected String getVnr() {
        return vnr;
    }
    protected void setVnr(String vnr) {
        this.vnr = vnr;
    }
    protected List<WeitereFahrer> getFahrer() {
        return fahrer;
    }
    protected void setFahrer(List<WeitereFahrer> fahrer) {
        this.fahrer = fahrer;
    }
    protected LocalDate getFaelligkeit() {
        return faelligkeit;
    }
    protected void setFaelligkeit(LocalDate faelligkeit) {
        this.faelligkeit = faelligkeit;
    }
    protected int getLz() {
        return lz;
    }
    protected void setLz(int lz) {
        this.lz = lz;
    }
    protected BigDecimal getRabatt() {
        return rabatt;
    }
    protected void setRabatt(BigDecimal rabatt) {
        this.rabatt = rabatt;
    }
    protected String getVermittler() {
        return vermittler;
    }
    protected void setVermittler(String vermittler) {
        this.vermittler = vermittler;
    }
    protected BigDecimal getZuschlag() {
        return zuschlag;
    }
    protected void setZuschlag(BigDecimal zuschlag) {
        this.zuschlag = zuschlag;
    }
    protected BigDecimal getRabattAbsolut() {
        return rabattAbsolut;
    }
    protected void setRabattAbsolut(BigDecimal rabattAbsolut) {
        this.rabattAbsolut = rabattAbsolut;
    }
    protected LocalDate getUrspruenglicherBeginn() {
        return urspruenglicherBeginn;
    }
    protected void setUrspruenglicherBeginn(LocalDate urspruenglicherBeginn) {
        this.urspruenglicherBeginn = urspruenglicherBeginn;
    }
    protected BigDecimal getIntervallBeitragGesamt() {
        return intervallBeitragGesamt;
    }
    protected void setIntervallBeitragGesamt(BigDecimal intervallBeitragGesamt) {
        this.intervallBeitragGesamt = intervallBeitragGesamt;
    }
    protected LocalDate getVertragsBeginn() {
        return vertragsBeginn;
    }
    protected void setVertragsBeginn(LocalDate vertragsBeginn) {
        this.vertragsBeginn = vertragsBeginn;
    }
    protected LocalDate getVertragsEnde() {
        return vertragsEnde;
    }
    protected void setVertragsEnde(LocalDate vertragsEnde) {
        this.vertragsEnde = vertragsEnde;
    }
    protected LocalDate getGueltigVon() {
        return gueltigVon;
    }
    protected void setGueltigVon(LocalDate gueltigVon) {
        this.gueltigVon = gueltigVon;
    }
    protected String getGeschaeftsvorfall() {
        return geschaeftsvorfall;
    }
    protected void setGeschaeftsvorfall(String geschaeftsvorfall) {
        this.geschaeftsvorfall = geschaeftsvorfall;
    }

}
