package com.projectbarbel.histo.persistence.mongo.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.projectbarbel.histo.persistence.api.BitemporalStamp;
import com.projectbarbel.histo.persistence.api.BitemporalStamp.Builder;
import com.projectbarbel.histo.persistence.api.EffectivePeriod;
import com.projectbarbel.histo.persistence.api.RecordPeriod;

public class BitemporalCodec implements Codec<BitemporalStamp> {

    @Override
    public void encode(BsonWriter writer, BitemporalStamp value, EncoderContext encoderContext) {
        assert writer != null && value != null && encoderContext != null;
        writer.writeStartDocument();
        BsonBinary binary = new BsonBinary(toByteArray(value.getVersionId()));
        writer.writeBinaryData("versionId", binary);
        writer.writeString("documentId", value.getDocumentId());
        writer.writeDateTime("from",
                Date.from(value.getEffectiveTime().getFrom().atStartOfDay(ZoneId.of("Z")).toInstant()).getTime());
        writer.writeDateTime("until",
                Date.from(value.getEffectiveTime().getUntil().atStartOfDay(ZoneId.of("Z")).toInstant()).getTime());
        writer.writeBinaryData("createdAt", new BsonBinary(toByteArray(value.getRecordTime().getCreatedAt())));
        writer.writeString("createdBy", value.getRecordTime().getCreatedBy());
        writer.writeBinaryData("inactivatedAt", new BsonBinary(toByteArray(value.getRecordTime().getInactivatedAt())));
        writer.writeString("status", value.getRecordTime().getState());
        writer.writeString("inactivatedBy", value.getRecordTime().getInactivatedBy());
        writer.writeString("activity", value.getActivity());
        writer.writeEndDocument();
    }

    @Override
    public Class<BitemporalStamp> getEncoderClass() {
        return BitemporalStamp.class;
    }

    @Override
    public BitemporalStamp decode(BsonReader reader, DecoderContext decoderContext) {
        assert reader != null && decoderContext != null;
        Builder builder = BitemporalStamp.builder();
        reader.readStartDocument();
        builder.withVersionId(fromByteArray(reader.readBinaryData("versionId").getData()));
        builder.withDocumentId(reader.readString("documentId"));
        builder.withEffectiveTime(new EffectivePeriod(
                Instant.ofEpochMilli(reader.readDateTime("from")).atZone(ZoneId.of("Z")).toLocalDate(),
                Instant.ofEpochMilli(reader.readDateTime("until")).atZone(ZoneId.of("Z")).toLocalDate()));
        LocalDateTime createdAt = (LocalDateTime) fromByteArray(reader.readBinaryData("createdAt").getData());
        String createdBy = reader.readString("createdBy");
        LocalDateTime inactivatedAt = (LocalDateTime) fromByteArray(reader.readBinaryData("inactivatedAt").getData());
        String state = reader.readString("status");
        String inactivatedBy = reader.readString("inactivatedBy");
        builder.withRecordTime(new RecordPeriod(ZoneId.systemDefault(), createdAt, createdBy, inactivatedAt,
                inactivatedBy, state));
        builder.withActivity(reader.readString("activity"));
        reader.readEndDocument();
        return builder.build();
    }

    private byte[] toByteArray(Object object) {
        ObjectOutput out = null;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();) {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            new RuntimeException("error writing object to byte array: " + object.getClass().getName(), e);
        }
        return null;
    }

    private Serializable fromByteArray(byte[] input) {
        ObjectInput in = null;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(input);) {
            in = new ObjectInputStream(bis);
            return (Serializable) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            new RuntimeException("error writing byte array to object", e);
        }
        return null;
    }

}
