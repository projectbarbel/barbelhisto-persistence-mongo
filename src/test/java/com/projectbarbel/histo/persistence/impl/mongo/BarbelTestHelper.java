package com.projectbarbel.histo.persistence.impl.mongo;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.projectbarbel.histo.functions.UUIDGenerator;
import org.projectbarbel.histo.model.BitemporalStamp;
import org.projectbarbel.histo.model.DefaultDocument;
import org.projectbarbel.histo.model.EffectivePeriod;
import org.projectbarbel.histo.model.RecordPeriod;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;

import io.github.benas.randombeans.EnhancedRandomBuilder;
import io.github.benas.randombeans.FieldDefinition;
import io.github.benas.randombeans.api.EnhancedRandom;

public class BarbelTestHelper {

    public static <T> T random(Class<T> clazz, String... excludedFields) {
        return EnhancedRandomBuilder.aNewEnhancedRandomBuilder().randomize(Serializable.class, new Supplier<String>() {

            @Override
            public String get() {
                return UUID.randomUUID().toString();
            }
        }).randomize(LocalDate.class, new Supplier<ZonedDateTime>() {

            @Override
            public ZonedDateTime get() {
                return BarbelTestHelper.randomLocalDateTime(2000, 2020);
            }
        }).randomize(EffectivePeriod.class, new Supplier<EffectivePeriod>() {

            @Override
            public EffectivePeriod get() {
                ZonedDateTime effectiveFrom = randomLocalDateTime(2000, 2020);
                return EffectivePeriod.of(effectiveFrom, randomLocalDateTime(effectiveFrom.plusDays(1), 2020));
            }
        }).randomize(RecordPeriod.class, new Supplier<RecordPeriod>() {

            @Override
            public RecordPeriod get() {
                return RecordPeriod.builder().build();
            }
        }).randomize(new FieldDefinition<BitemporalStamp, Object>("versionId", Object.class, BitemporalStamp.class),
                new Supplier<Object>() {

                    @Override
                    public Object get() {
                        return new UUIDGenerator().get();
                    }
                })
                .randomize(
                        new FieldDefinition<BitemporalStamp, Object>("documentId", Object.class, BitemporalStamp.class),
                        new Supplier<Object>() {

                            @Override
                            public Object get() {
                                return new UUIDGenerator().get();
                            }
                        })
                .build().nextObject(clazz, excludedFields);
    }

    public static List<BitemporalStamp> generateListOfBitemporals(String docId, List<ZonedDateTime> effectiveDates) {
        List<BitemporalStamp> journal = new ArrayList<BitemporalStamp>();
        for (int i = 0; i < effectiveDates.size(); i++) {
            journal.add(createPeriod(docId, effectiveDates, i));
        }
        return journal;
    }

    private static BitemporalStamp createPeriod(String docId, List<ZonedDateTime> effectiveDates, int listPointer) {
        return BitemporalStamp.builder().withDocumentId(docId)
                .withEffectiveTime(effectiveDates.size() - 1 == listPointer
                        ? EffectivePeriod.of(effectiveDates.get(listPointer),
                                ZonedDateTime.of(LocalDateTime.MAX, ZoneId.systemDefault()))
                        : EffectivePeriod.of(effectiveDates.get(listPointer), effectiveDates.get(listPointer + 1)))
                .withRecordTime(RecordPeriod.builder().build()).build();
    }

    @SuppressWarnings("unchecked")
    public static <T> IndexedCollection<T> generateJournalOfDefaultDocuments(String docId,
            List<ZonedDateTime> effectiveDates) {
        IndexedCollection<T> journal = new ConcurrentIndexedCollection<T>();
        for (int i = 0; i < effectiveDates.size(); i++) {
            journal.add((T) DefaultDocument.builder().withBitemporalStamp(createPeriod(docId, effectiveDates, i))
                    .withData(EnhancedRandom.random(String.class)).withId(docId).build());
        }
        return journal;
    }

    @SuppressWarnings("unchecked")
    public static <T> IndexedCollection<T> asIndexedCollection(T... objects) {
        ConcurrentIndexedCollection<T> collection = new ConcurrentIndexedCollection<>();
        Arrays.asList(objects).stream().forEach(d -> collection.add((T) d));
        return collection;
    }

    public static ZonedDateTime randomLocalDateTime(int startYear, int endYear) {
        long minDay = LocalDate.of(startYear, 1, 1).toEpochDay();
        long maxDay = LocalDate.of(endYear, 12, 31).toEpochDay();
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return ZonedDateTime.of(LocalDateTime.of(LocalDate.ofEpochDay(randomDay), LocalTime.of(0, 0)),
                ZoneId.systemDefault());
    }

    public static ZonedDateTime randomLocalDateTime(ZonedDateTime low, int highYear) {
        long minDay = low.toLocalDate().toEpochDay();
        long maxDay = LocalDate.of(highYear, 12, 31).toEpochDay();
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return ZonedDateTime.of(LocalDateTime.of(LocalDate.ofEpochDay(randomDay), LocalTime.of(0, 0)),
                ZoneId.systemDefault());
    }

    public static LocalDate randomLocalDate(LocalDate low, LocalDate high) {
        long minDay = low.toEpochDay();
        long maxDay = high.toEpochDay();
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return LocalDate.ofEpochDay(randomDay);
    }

    public static Instant randomInstant(int startYear, int endYear) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        BiFunction<Integer, Integer, Integer> rdm = random::nextInt;
        return LocalDateTime.of(rdm.apply(startYear, endYear), rdm.apply(1, 12), rdm.apply(1, 28), rdm.apply(0, 23),
                rdm.apply(0, 60), rdm.apply(0, 60), rdm.apply(0, 999999999)).toInstant(ZoneOffset.UTC);
    }

}
