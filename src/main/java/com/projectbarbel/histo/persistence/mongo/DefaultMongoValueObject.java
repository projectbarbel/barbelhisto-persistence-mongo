package com.projectbarbel.histo.persistence.mongo;

import java.util.Objects;

import javax.annotation.Generated;

import org.bson.types.ObjectId;

import com.projectbarbel.histo.persistence.api.BitemporalStamp;

public class DefaultMongoValueObject {

    private ObjectId objectId;

    public void setObjectId(ObjectId objectId) {
        this.objectId = objectId;
    }

    public void setBitemporalStamp(BitemporalStamp bitemporalStamp) {
        this.bitemporalStamp = bitemporalStamp;
    }

    public void setData(String data) {
        this.data = data;
    }

    private BitemporalStamp bitemporalStamp;
    private String data;

    @Generated("SparkTools")
    private DefaultMongoValueObject(Builder builder) {
        this.objectId = builder.objectId;
        this.bitemporalStamp = builder.bitemporalStamp;
        this.data = builder.data;
    }

    public String getData() {
        return data;
    }

    public DefaultMongoValueObject() {
    }

    public DefaultMongoValueObject(ObjectId id, BitemporalStamp bitemporalStamp, String data) {
        super();
        this.objectId = id;
        this.bitemporalStamp = bitemporalStamp;
        this.data = data;
    }

    public BitemporalStamp getBitemporalStamp() {
        return bitemporalStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DefaultMongoValueObject)) {
            return false;
        }
        DefaultMongoValueObject defaultValueObject = (DefaultMongoValueObject) o;
        return Objects.equals(objectId, defaultValueObject.getVersionId())
                && Objects.equals(data, defaultValueObject.getData())
                && Objects.equals(bitemporalStamp, defaultValueObject.getBitemporalStamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, bitemporalStamp, data);
    }

    @Override
    public String toString() {
        return "DefaultMongoValueObject [id=" + objectId + ", bitemporalStamp=" + bitemporalStamp + ", data=" + data + "]";
    }

    public ObjectId getVersionId() {
        return objectId;
    }
    
    public ObjectId getId() {
        return objectId;
    }

    public ObjectId getObjectId() {
        return objectId;
    }

    /**
     * Creates builder to build {@link DefaultMongoValueObject}.
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to build {@link DefaultMongoValueObject}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private ObjectId objectId;
        private BitemporalStamp bitemporalStamp;
        private String data;

        private Builder() {
        }

        public Builder withObjectId(ObjectId objectId) {
            this.objectId = objectId;
            return this;
        }

        public Builder withBitemporalStamp(BitemporalStamp bitemporalStamp) {
            this.bitemporalStamp = bitemporalStamp;
            return this;
        }

        public Builder withData(String data) {
            this.data = data;
            return this;
        }

        public DefaultMongoValueObject build() {
            return new DefaultMongoValueObject(this);
        }
    }
    
}
