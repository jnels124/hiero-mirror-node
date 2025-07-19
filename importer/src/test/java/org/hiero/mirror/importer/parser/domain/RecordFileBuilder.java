// SPDX-License-Identifier: Apache-2.0

package org.hiero.mirror.importer.parser.domain;

import static org.hiero.mirror.importer.domain.StreamFilename.FileType.DATA;

import jakarta.inject.Named;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import org.hiero.mirror.common.domain.DomainBuilder;
import org.hiero.mirror.common.domain.StreamType;
import org.hiero.mirror.common.domain.transaction.RecordFile;
import org.hiero.mirror.common.domain.transaction.RecordItem;
import org.hiero.mirror.common.domain.transaction.TransactionType;
import org.hiero.mirror.importer.domain.StreamFilename;
import org.hiero.mirror.importer.parser.domain.RecordItemBuilder.TransferType;
import org.hiero.mirror.importer.test.performance.PerformanceProperties.SubType;
import org.springframework.util.Assert;

/**
 * Generates record files using pre-defined templates for bulk creation of record items.
 */
@CustomLog
@Named
@RequiredArgsConstructor
public class RecordFileBuilder {

    private final DomainBuilder domainBuilder;
    private final RecordItemBuilder recordItemBuilder;

    public Builder recordFile() {
        return new Builder();
    }

    public class Builder {

        private static final long CLOSE_INTERVAL =
                StreamType.RECORD.getFileCloseInterval().toNanos();

        private final List<ItemBuilder> itemBuilders = new ArrayList<>();

        private RecordFile previous;

        private Builder() {}

        public RecordFile build() {
            Assert.notEmpty(itemBuilders, "Must contain at least one record item");
            var recordFile = domainBuilder.recordFile();
            var recordItems = new ArrayList<RecordItem>();
            recordFile.customize(r -> r.items(recordItems));

            if (previous != null) {
                var now = Instant.ofEpochSecond(0, previous.getConsensusStart() + CLOSE_INTERVAL);
                recordItemBuilder.setNow(now);
            }

            for (var itemBuilder : itemBuilders) {
                var supplier = itemBuilder.build();
                RecordItem recordItem;

                while ((recordItem = supplier.get()) != null) {
                    recordItems.add(recordItem);
                }
            }

            var consensusEnd = recordItems.getLast().getConsensusTimestamp();
            var consensusStart = recordItems.getFirst().getConsensusTimestamp();
            Instant instant = Instant.ofEpochSecond(0, consensusStart);
            String filename = StreamFilename.getFilename(StreamType.RECORD, DATA, instant) + ".gz";

            recordFile.customize(r -> {
                r.consensusEnd(consensusEnd)
                        .consensusStart(consensusStart)
                        .count((long) recordItems.size())
                        .index(0L)
                        .name(filename);
                if (previous != null) {
                    r.index(previous.getIndex() + 1).previousHash(previous.getHash());
                }
            });

            var current = recordFile.get();
            previous = current;
            return current;
        }

        public Builder previous(RecordFile recordFile) {
            this.previous = recordFile;
            return this;
        }

        public Builder recordItem(Function<RecordItemBuilder, RecordItemBuilder.Builder<?>> template) {
            return recordItems(i -> i.count(1).template(template));
        }
        //        public Builder recordItem(Supplier<RecordItemBuilder.Builder<?>> recordItem) {
        //            return recordItems(i -> i.count(1).entities(1, r -> recordItem.get()));
        //        }

        public Builder recordItem(TransactionType type) {
            return recordItems(i -> i.count(1).type(type));
        }

        public Builder recordItems(Consumer<ItemBuilder> recordItems) {
            var itemBuilder = new ItemBuilder();
            recordItems.accept(itemBuilder);
            itemBuilders.add(itemBuilder);
            return this;
        }
    }

    public class ItemBuilder {

        private int count = 100;
        private int entities = 0;
        private Function<RecordItemBuilder, RecordItemBuilder.Builder<?>> entityTemplate;
        private boolean entityAutoCreation = false;
        private SubType subType = SubType.STANDARD;
        private TransactionType type = TransactionType.UNKNOWN;
        private Function<RecordItemBuilder, RecordItemBuilder.Builder<?>> template;

        //        @Getter(lazy = true, value = AccessLevel.PRIVATE)
        //        private final List<RecordItemBuilder.Builder<?>> builders = createBuilders();

        private ItemBuilder() {}

        public ItemBuilder count(int count) {
            Assert.isTrue(count > 0, "count must be positive");
            this.count = count;
            return this;
        }

        public ItemBuilder entities(
                int entities, Function<RecordItemBuilder, RecordItemBuilder.Builder<?>> entityTemplate) {
            Assert.isTrue(entities > 0, "entities must be positive");
            this.entities = entities;
            this.entityTemplate = entityTemplate;
            return this;
        }

        public ItemBuilder entityAutoCreation(boolean entityAutoCreation) {
            this.entityAutoCreation = entityAutoCreation;
            return this;
        }

        public ItemBuilder subType(SubType subType) {
            Assert.notNull(subType, "subType must not be null");
            this.subType = subType;
            return this;
        }

        public ItemBuilder template(Function<RecordItemBuilder, RecordItemBuilder.Builder<?>> template) {
            Assert.notNull(template, "template must not be null");
            this.template = template;
            return this;
        }

        //        public ItemBuilder template(Supplier<RecordItemBuilder.Builder<?>> template) {
        //            Assert.notNull(template, "template must not be null");
        //            this.template = template;
        //            return this;
        //        }

        public ItemBuilder type(TransactionType type) {
            Assert.notNull(type, "type must not be null");
            Assert.isTrue(type != TransactionType.UNKNOWN, "type must not be unknown");
            this.type = type;
            return this;
        }

        private Supplier<RecordItem> build() {
            var entityBuilders = createEntityBuilders();
            var templateBuilder = buildTemplate();
            Assert.isTrue(
                    !entityBuilders.isEmpty() || templateBuilder != null, "entityBuilders and template are both null");

            var counter = new AtomicInteger(count);
            var creates = new ArrayDeque<>(
                    entityAutoCreation ? recordItemBuilder.getCreateTransactions() : Collections.emptyList());

            return () -> {
                if (!creates.isEmpty()) {
                    var builder = creates.remove();
                    var recordItem = builder.build();
                    log.info("Creating {}", TransactionType.of(recordItem.getTransactionType()));
                    return recordItem;
                }

                int remaining = counter.getAndDecrement();
                if (remaining <= 0) {
                    return null;
                }

                if (!entityBuilders.isEmpty()) {
                    var index = (count - remaining) % entityBuilders.size();
                    var builder = entityBuilders.get(index);
                    return builder.build();
                } else {
                    return templateBuilder.apply(recordItemBuilder).build();
                }
            };
        }

        private Function<RecordItemBuilder, RecordItemBuilder.Builder<?>> buildTemplate() {
            if (template != null) {
                return template;
            }

            if (subType != SubType.STANDARD) {
                return switch (subType) {
                    case TOKEN_TRANSFER -> (recordItemBuilder) -> recordItemBuilder.cryptoTransfer(TransferType.TOKEN);
                    default -> throw new IllegalArgumentException("subType not supported: " + subType);
                };
            }

            if (type == TransactionType.UNKNOWN) {
                throw new IllegalArgumentException("type must not be unknown");
            }

            return r -> recordItem(type).get();
        }

        private List<RecordItemBuilder.Builder<?>> createEntityBuilders() {
            var builderList = new ArrayList<RecordItemBuilder.Builder<?>>();
            for (int i = 0; i < entities; i++) {
                builderList.add(entityTemplate.apply(recordItemBuilder));
            }

            return builderList;
        }

        private Supplier<RecordItemBuilder.Builder<?>> recordItem(TransactionType transactionType) {
            var supplier = recordItemBuilder.lookup(transactionType);
            if (supplier == null) {
                throw new UnsupportedOperationException("Transaction type not supported: " + transactionType);
            }
            return supplier;
        }
    }
}
