package com.projectbarbel.histo.persistence.mongo;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.projectbarbel.histo.model.BitemporalStamp;
import org.projectbarbel.histo.model.BitemporalVersion;

import com.google.gson.Gson;

public class BitemporalVersionCodec implements Codec<BitemporalVersion> {

    private Class<?> managedType;
    private Gson gson;
    
    public BitemporalVersionCodec(Class<?> managedType, Gson gson) {
        this.managedType = managedType;
        this.gson = gson;
    }
    
    @Override
    public void encode(BsonWriter writer, BitemporalVersion value, EncoderContext encoderContext) {
        assert writer != null && value != null && encoderContext != null;
        writer.writeStartDocument("bitemporalStamp");
        writer.writeString(gson.toJson(value.getBitemporalStamp()));
        writer.writeEndDocument();
        writer.writeStartDocument("bitemporalStamp");
        writer.writeString("object", gson.toJson(value.getObject()));
        writer.writeEndDocument();
        writer.writeStartDocument("bitemporalStamp");
        writer.writeString("objectType", value.getObjectType());
        writer.writeEndDocument();
    }

    @Override
    public Class<BitemporalVersion> getEncoderClass() {
        return BitemporalVersion.class;
    }

    @Override
    public BitemporalVersion decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument();
        reader.readObjectId();
        reader.readStartDocument();
        BitemporalStamp stamp = gson.fromJson(reader.readBsonType().toString(), BitemporalStamp.class);
        reader.readEndDocument();
        reader.readStartDocument();
        Object object = gson.fromJson(reader.readString("object"), managedType);
        reader.readEndDocument();
        return new BitemporalVersion(stamp, object);
    }

}
