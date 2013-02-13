package org.atlasapi.persistence.content.elasticsearch.support;

import static org.atlasapi.content.criteria.attribute.Attributes.ALIASES_NAMESPACE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.content.criteria.AtomicQuerySet;
import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.content.criteria.StringAttributeQuery;
import org.atlasapi.content.criteria.attribute.Attribute;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.content.criteria.attribute.StringValuedAttribute;
import org.atlasapi.content.criteria.operator.Operators;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.elasticsearch.schema.EsContent;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class EsQueryBuilderTest {

    private static final String INDEX = "test";
    private static final String TYPE = "test";

    private final EsQueryBuilder builder = new EsQueryBuilder();
    
    private final Node esClient = NodeBuilder.nodeBuilder()
        .local(true).clusterName(UUID.randomUUID().toString())
        .build().start();
    
    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }
    
    @After
    public void tearDown() throws Exception {
        IndicesStatusRequest req = Requests.indicesStatusRequest((String[]) null);
        IndicesStatusResponse statuses = esClient.client().admin().indices().status(req).actionGet();
        for (String index : statuses.getIndices().keySet()) {
            esClient.client().admin().indices().delete(Requests.deleteIndexRequest(index)).actionGet();
        }
        esClient.close();
    }
    
    @Before
    public void setup() throws Exception {
//        createIndex(esClient, INDEX).actionGet();
//        
//        putMapping(esClient, INDEX, TYPE, XContentFactory.jsonBuilder().startObject()
//            .startObject(TYPE)
//                .startObject("properties")
//                    .startObject("id")
//                        .field("type").value("long")
//                    .endObject()
//                    .startObject("aliases")
//                        .field("type").value("nested")
//                        .startObject("properties")
//                            .startObject("namespace")
//                                .field("type").value("string")
//                                .field("index").value("not_analyzed")
//                            .endObject()
//                            .startObject("value")
//                                .field("type").value("string")
//                                .field("index").value("not_analyzed")
//                            .endObject()
//                            .startObject("deeper")
//                                .field("type").value("nested")
//                                .startObject("properties")
//                                    .startObject("name")
//                                        .field("type").value("string")
//                                        .field("index").value("not_analyzed")
//                                    .endObject()
//                                    .startObject("value")
//                                        .field("type").value("string")
//                                        .field("index").value("not_analyzed")
//                                    .endObject()
//                                .endObject()
//                            .endObject()
//                        .endObject()
//                    .endObject()
//                .endObject()
//            .endObject()
//        .endObject()).actionGet();
//
//        index(esClient, INDEX, TYPE, "one", ImmutableMap.<String,Object>of(
//            "id", 1, 
//            "aliases", ImmutableList.of(
//                ImmutableMap.of("namespace", "nsone","value", "vone"),
//                ImmutableMap.of("namespace", "nstwo","value", "vtwo"),
//                ImmutableMap.of("namespace", "ns3","value", "v3",
//                    "deeper", ImmutableList.of(
//                        ImmutableMap.of("name","nname", "value","nval")
//                    )
//                )
//            )
//        )).actionGet();
//        
//        System.out.println(esClient.client().get(Requests.getRequest(INDEX).type(TYPE).id("one")).actionGet().getSourceAsString());
//        
//        Thread.sleep(1000);
        
    }
    
    @Test
    public void testBuildQuery() throws Exception {

        AtomicQuerySet querySet = new AtomicQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1)))
        ));
//        System.out.println(queryHits(querySet).getTotalHits());
//        //assertThat(queryHits(querySet).getTotalHits(), is(1L));
//        
//        querySet = new AtomicQuerySet(ImmutableList.of(
//            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1))),
//            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("nsone"))
//        ));
//        System.out.println(queryHits(querySet).getTotalHits());
//        //assertThat(queryHits(querySet).getTotalHits(), is(1L));
//
//        querySet = new AtomicQuerySet(ImmutableList.of(
//            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1))),
//            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("nsone")),
//            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("vone"))
//        ));
//        System.out.println(queryHits(querySet).getTotalHits());
//        //assertThat(queryHits(querySet).getTotalHits(), is(1L));
//
//        querySet = new AtomicQuerySet(ImmutableList.of(
//            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1))),
//            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("nsone")),
//            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("vtwo"))
//        ));
//        System.out.println(queryHits(querySet).getTotalHits());
//        //assertThat(queryHits(querySet).getTotalHits(), is(0L));

        querySet = new AtomicQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1))),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3"))/*,
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval"))*/
        ));

//        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
//            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1))),
//            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
//            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
//            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
//            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3"))
//        );
//        NodeSet set = new NodeSet();
//        for (AttributeQuery<?> attributeQuery : queryList) {
//            set.addQuery(attributeQuery);
//            System.out.println(set.toString());
//        }
        
//        System.out.println(queryHits(querySet).getTotalHits());
//        assertThat(queryHits(querySet).getTotalHits(), is(0L));
    }
    
    @Test
    public void testAddingToLongTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
        }
        System.out.println(set.toString());
    }

    @Test
    public void testAddingToLongerTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_THIRD.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
        }
        System.out.println(set.toString());
    }

    @Test
    public void testAddingLongToLongerTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("nname"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
            System.out.println(set.toString());
        }
    }
    
    @Test
    public void testAddingLongerToShorterTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
        }
        System.out.println(set.toString());
    }
    
    @Test
    public void testAddingShorterToLongerTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
        }
        System.out.println(set.toString());
    }
    
    @Test
    public void testAddingLongToLongAndShortTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
            System.out.println(set.toString());
        }
    }

    @Test
    public void testAddingShorterToLongerIntermediate() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
        }
        System.out.println(set.toString());
    }

    @Test
    public void testSplitIntermediate() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
            System.out.println(set.toString());
        }
    }

    @Test
    public void testAgain() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("lname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("lval")),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3")),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
            System.out.println(set.toString());
        }
    }

    @Test
    public void testOnceAgain() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("lname")),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("lval")),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet();
        for (AttributeQuery<?> attributeQuery : queryList) {
            set.addQuery(attributeQuery);
            System.out.println(set.toString());
        }
    }
    
    @Test
    public void testAddingAttributesInAnyOrder() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("lname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("lval")),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3"))
        );
        for (List<AttributeQuery<?>> queries : Collections2.permutations(queryList)) {
            System.out.println(queries);
            System.out.println(new NodeSet(queries).toString());
        }
    }
    
//    @Test
//    public void testBuild() throws Exception {
//        QueryBuilder query2 = QueryBuilders.boolQuery()
//            .must(QueryBuilders.termsQuery("id",new int[]{1}))
//            .must(QueryBuilders.nestedQuery("aliases", 
//                QueryBuilders.termsQuery("aliases.namespace", "nsone")
////                QueryBuilders.boolQuery()
////                    .must(QueryBuilders.termsQuery("aliases.namespace", "nsone"))
////                    .must(QueryBuilders.termsQuery("aliases.value", "vone"))
//            ))
//            .must(QueryBuilders.nestedQuery("aliases",
//                QueryBuilders.boolQuery()
//                    .must(QueryBuilders.termsQuery("aliases.namespace", "ns3"))
//                    .must(QueryBuilders.termsQuery("aliases.value", "v3"))
//                    .must(QueryBuilders.nestedQuery("aliases.deeper", 
//                        QueryBuilders.boolQuery()
//                            .must(QueryBuilders.termsQuery("aliases.deeper.name", "nname"))
//                            .must(QueryBuilders.termsQuery("aliases.deeper.value", "nval"))
//                    ))
//            ));
//        
//        System.out.println(query2);
//        
//        SearchResponse execute2 = esClient.client().prepareSearch().setQuery(query2).execute().get();
//        System.out.println(execute2.getHits().getTotalHits());
//        System.out.println(execute2.getHits().getAt(0).id());
//    }
    
    private final Attribute<String> ALIAS_DEEPER_NAME = 
        new StringValuedAttribute("aliases.deeper.name", Identified.class, true);
    private final Attribute<String> ALIAS_DEEPER_VALUE = 
        new StringValuedAttribute("aliases.deeper.value", Identified.class, true);
    private final Attribute<String> ALIAS_LONGER_NAME = 
        new StringValuedAttribute("aliases.deeper.longer.name", Identified.class, true);
    private final Attribute<String> ALIAS_LONGER_VALUE = 
        new StringValuedAttribute("aliases.deeper.longer.value", Identified.class, true);
    private final Attribute<String> ALIAS_LONGER_THIRD = 
        new StringValuedAttribute("aliases.deeper.longer.third", Identified.class, true);
    
    
    private SearchHits queryHits(AtomicQuerySet query) throws Exception {
        return esClient.client()
            .prepareSearch()
            .setQuery(print(builder.buildQuery(query)))
            .execute().get().getHits();
    }

    private Map<String, Object> print(Map<String, Object> map) {
        System.out.println(map);
        return map;
    }

    private ActionFuture<IndexResponse> index(Node esClient, String index, String type,
                       String id, ImmutableMap<String,Object> source) {
        return esClient.client().index(Requests.indexRequest()
            .index(INDEX)
            .type(TYPE)
            .id(id).source(
                source
            )
        );
    }

    private ActionFuture<PutMappingResponse> putMapping(Node client, String index, String type, XContentBuilder mapping) {
        return esClient.client()
            .admin()
            .indices()
            .putMapping(Requests
                .putMappingRequest(INDEX)
                .type(TYPE)
                .source(mapping)
            );
    }

    private ActionFuture<CreateIndexResponse> createIndex(Node client, String index) {
        return client.client()
            .admin()
            .indices()
            .create(Requests.createIndexRequest(index));
    }

}
