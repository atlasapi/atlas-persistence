package org.atlasapi.persistence.content.people.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.content.PeopleListerListener;
import org.atlasapi.persistence.content.people.PersonWriter;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.persistence.content.people.PeopleLister;
import org.atlasapi.persistence.content.people.PeopleResolver;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;
import org.atlasapi.serialization.json.configuration.model.FilteredContentGroupConfiguration;

/**
 *
 */
public class CassandraPersonStore implements PersonWriter, PeopleResolver, PeopleLister {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;

    public CassandraPersonStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.mapper.setFilters(new SimpleFilterProvider().addFilter(FilteredContentGroupConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredContentGroupConfiguration.CONTENTS_FILTER)));
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
    }

    @Override
    public void updatePersonItems(Person person) {
        try {
            MutationBatch mutation = keyspace.prepareMutationBatch();
            mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
            marshalPersonContents(person, mutation);
            Future<OperationResult<Void>> result = mutation.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public void createOrUpdatePerson(Person person) {
        try {
            MutationBatch mutation = keyspace.prepareMutationBatch();
            mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
            marshalPerson(person, mutation);
            marshalPersonContents(person, mutation);
            Future<OperationResult<Void>> result = mutation.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Person person(String uri) {
        try {
            Future<OperationResult<ColumnList<String>>> result = keyspace.prepareQuery(PEOPLE_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getKey(uri).
                    executeAsync();
            OperationResult<ColumnList<String>> columns = result.get(requestTimeout, TimeUnit.MILLISECONDS);
            if (!columns.getResult().isEmpty()) {
                return unmarshalFullPerson(columns.getResult());
            } else {
                return null;
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public void list(PeopleListerListener handler) {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(PEOPLE_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getAllRows();
            allRowsQuery.setRowLimit(100);
            //
            OperationResult<Rows<String, String>> result = allRowsQuery.execute();
            Iterator<Person> people = Iterators.filter(Iterators.transform(result.getResult().iterator(), new Function<Row, Person>() {

                @Override
                public Person apply(Row input) {
                    try {
                        if (!input.getColumns().isEmpty()) {
                            return unmarshalFullPerson(input.getColumns());
                        } else {
                            return null;
                        }
                    } catch (Exception ex) {
                        return null;
                    }
                }
            }), Predicates.notNull());
            //
            while (people.hasNext()) {
                Person person = people.next();
                handler.personListed(person);
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private void marshalPerson(Person person, MutationBatch mutation) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(person);
        mutation.withRow(PEOPLE_CF, person.getCanonicalUri()).
                putColumn(PERSON_COLUMN, bytes, null);
    }

    private void marshalPersonContents(Person person, MutationBatch mutation) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(person.getContents());
        mutation.withRow(PEOPLE_CF, person.getCanonicalUri()).
                putColumn(CONTENTS_COLUMN, bytes, null);
    }

    private Person unmarshalFullPerson(ColumnList<String> columns) throws IOException {
        Person person = mapper.readValue(columns.getColumnByName(PERSON_COLUMN).getByteArrayValue(), Person.class);
        List<ChildRef> children = mapper.readValue(columns.getColumnByName(CONTENTS_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, ChildRef.class));
        person.setContents(children);
        return person;
    }
}
