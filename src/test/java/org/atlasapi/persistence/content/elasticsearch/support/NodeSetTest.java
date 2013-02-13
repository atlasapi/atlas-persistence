package org.atlasapi.persistence.content.elasticsearch.support;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Queue;

import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.content.criteria.attribute.Attribute;
import org.atlasapi.content.criteria.attribute.StringValuedAttribute;
import org.atlasapi.content.criteria.operator.Operators;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.elasticsearch.support.NodeSet.QueryNode;
import org.atlasapi.persistence.content.elasticsearch.support.NodeSet.QueryNode.IntermediateNode;
import org.atlasapi.persistence.content.elasticsearch.support.NodeSet.QueryNode.TerminalNode;
import org.atlasapi.persistence.content.elasticsearch.support.NodeSet.QueryNodeVisitor;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class NodeSetTest {
    
//    private static final String INDEX = "test";
//    private static final String TYPE = "test";
//
//    private final EsQueryBuilder builder = new EsQueryBuilder();
//    
//    private final Node esClient = NodeBuilder.nodeBuilder()
//        .local(true).clusterName(UUID.randomUUID().toString())
//        .build().start();
//    
//    @BeforeClass
//    public static void before() throws Exception {
//        Logger root = Logger.getRootLogger();
//        root.addAppender(new ConsoleAppender(
//            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
//        root.setLevel(Level.WARN);
//    }
//    
//    @After
//    public void tearDown() throws Exception {
//        IndicesStatusRequest req = Requests.indicesStatusRequest((String[]) null);
//        IndicesStatusResponse statuses = esClient.client().admin().indices().status(req).actionGet();
//        for (String index : statuses.getIndices().keySet()) {
//            esClient.client().admin().indices().delete(Requests.deleteIndexRequest(index)).actionGet();
//        }
//        esClient.close();
//    }
    
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

//        AtomicQuerySet querySet = new AtomicQuerySet(ImmutableList.of(
//            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1)))
//        ));
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

        /*querySet = new AtomicQuerySet(ImmutableList.of(
            Attributes.ID.createQuery(Operators.EQUALS, ImmutableList.of(Id.valueOf(1))),
            Attributes.ALIASES_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            Attributes.ALIASES_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3")),
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        ));*/

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
    
    private final Attribute<String> ID = 
        new StringValuedAttribute("id", Identified.class, true);
    private final Attribute<String> ALIAS_NAMESPACE = 
        new StringValuedAttribute("aliases.namespace", Identified.class, true);
    private final Attribute<String> ALIAS_VALUE = 
        new StringValuedAttribute("aliases.value", Identified.class, true);
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
    
    @Test
    public void testAddingUncommonPrefix() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ID.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 3))).and(path())),
            terminalNode(both(query(queryList.get(0))).and(path("aliases","namespace"))),
            terminalNode(both(query(queryList.get(1))).and(path("id")))
        ));
    }
    
    @Test
    public void testAddingEqualToLongTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases", "deeper"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(1))).and(path("value")))
        ));
    }

    @Test
    public void testAddingToLongerTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_THIRD.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());

        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 5))).and(path("aliases","deeper","longer"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(2))).and(path("third"))),
            terminalNode(both(query(queryList.get(1))).and(path("value")))
        ));
    }

    @Test
    public void testAddingLongToLongerTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("nname"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases"))),
            intermediateNode(both(children(nodes.subList(4, 6))).and(path("deeper","longer"))),
            terminalNode(both(query(queryList.get(2))).and(path("namespace"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(1))).and(path("value")))
        ));
    }
    
    @Test
    public void testAddingLongerToShorterTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases","deeper"))),
            terminalNode(both(query(queryList.get(1))).and(path("longer","name"))),
            terminalNode(both(query(queryList.get(0))).and(path("name")))
        ));
    }
    
    @Test
    public void testAddingShorterToLongerTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases", "deeper"))),
            terminalNode(both(query(queryList.get(0))).and(path("longer", "name"))),
            terminalNode(both(query(queryList.get(1))).and(path("name")))
        ));
    }
    
    @Test
    public void testAddingLongToLongAndShortTerminal() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>>of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases"))),
            intermediateNode(both(children(nodes.subList(4, 6))).and(path("deeper"))),
            terminalNode(both(query(queryList.get(1))).and(path("namespace"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(2))).and(path("value")))
        ));
    }

    @Test
    public void testAddingShorterToLongerIntermediate() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases"))), 
            intermediateNode(both(children(nodes.subList(4, 6))).and(path("deeper"))), 
            terminalNode(both(query(queryList.get(2))).and(path("namespace"))), 
            terminalNode(both(query(queryList.get(0))).and(path("name"))), 
            terminalNode(both(query(queryList.get(1))).and(path("value")))
        ));
    }

    @Test
    public void testSplitIntermediate() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 4))).and(path("aliases"))),
            intermediateNode(both(children(nodes.subList(4, 7))).and(path("deeper"))),
            terminalNode(both(query(queryList.get(2))).and(path("namespace"))),
            intermediateNode(both(children(nodes.subList(7, 9))).and(path("longer"))),
            terminalNode(both(query(queryList.get(3))).and(path("name"))),
            terminalNode(both(query(queryList.get(4))).and(path("value"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(1))).and(path("value")))
        ));
    }

    @Test
    public void testAgain() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("lname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("lval")),
            ALIAS_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 5))).and(path("aliases"))),
            intermediateNode(both(children(nodes.subList(5, 8))).and(path("deeper"))),
            terminalNode(both(query(queryList.get(5))).and(path("namespace"))),
            terminalNode(both(query(queryList.get(4))).and(path("value"))),
            intermediateNode(both(children(nodes.subList(8, 10))).and(path("longer"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(1))).and(path("value"))),
            terminalNode(both(query(queryList.get(2))).and(path("name"))),
            terminalNode(both(query(queryList.get(3))).and(path("value")))
        ));
    }

    @Test
    public void testOnceAgain() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("lname")),
            ALIAS_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("lval")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3"))
        );
        NodeSet set = new NodeSet(queryList);
        List<QueryNode> nodes = set.accept(new NodeListingVisitor());
        
        matchesInOrder(nodes, ImmutableList.of(
            intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
            intermediateNode(both(children(nodes.subList(2, 5))).and(path("aliases"))),
            intermediateNode(both(children(nodes.subList(5, 8))).and(path("deeper"))),
            terminalNode(both(query(queryList.get(5))).and(path("namespace"))),
            terminalNode(both(query(queryList.get(3))).and(path("value"))),
            intermediateNode(both(children(nodes.subList(8, 10))).and(path("longer"))),
            terminalNode(both(query(queryList.get(0))).and(path("name"))),
            terminalNode(both(query(queryList.get(1))).and(path("value"))),
            terminalNode(both(query(queryList.get(2))).and(path("name"))),
            terminalNode(both(query(queryList.get(4))).and(path("value")))
        ));
    }
    
    @Test
    public void testAddingAttributesInAnyOrder() {
        List<AttributeQuery<?>> queryList = ImmutableList.<AttributeQuery<?>> of(
            ALIAS_DEEPER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("nname")),
            ALIAS_DEEPER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("nval")),
            ALIAS_LONGER_NAME.createQuery(Operators.EQUALS, ImmutableList.of("lname")),
            ALIAS_LONGER_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("lval")),
            ALIAS_NAMESPACE.createQuery(Operators.EQUALS, ImmutableList.of("ns3")),
            ALIAS_VALUE.createQuery(Operators.EQUALS, ImmutableList.of("v3"))
        );
        for (List<AttributeQuery<?>> queries : Collections2.permutations(queryList)) {
            NodeSet set = new NodeSet(queries);
            List<QueryNode> nodes = set.accept(new NodeListingVisitor());
            
            matchesInOrder(nodes, ImmutableList.of(
                intermediateNode(both(children(nodes.subList(1, 2))).and(path())),
                intermediateNode(both(children(nodes.subList(2, 5))).and(path("aliases"))),
                intermediateNode(both(children(nodes.subList(5, 8))).and(path("deeper"))),
                terminalNode(both(query(queryList.get(4))).and(path("namespace"))),
                terminalNode(both(query(queryList.get(5))).and(path("value"))),
                intermediateNode(both(children(nodes.subList(8, 10))).and(path("longer"))),
                terminalNode(both(query(queryList.get(0))).and(path("name"))),
                terminalNode(both(query(queryList.get(1))).and(path("value"))),
                terminalNode(both(query(queryList.get(2))).and(path("name"))),
                terminalNode(both(query(queryList.get(3))).and(path("value")))
            ));
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
    
    
    public void matchesInOrder(List<QueryNode> nodes, List<Matcher<QueryNode>> matchers) {
        assertThat("mismatched node and matcher count",
            nodes.size(), is(matchers.size()));
        for(int i = 0; i < nodes.size(); i++) {
            QueryNode node = nodes.get(i);
            assertThat(String.format("mismatch at pos %s: ", i), node, matchers.get(i));
        }
    }
    
    public static final class NodeListingVisitor implements QueryNodeVisitor<List<QueryNode>> {

        List<QueryNode> queries = Lists.newLinkedList();
        Queue<QueryNode> processing = Lists.newLinkedList(); 

        @Override
        public List<QueryNode> visit(IntermediateNode node) {
            queries.add(node);
            for (QueryNode desc : QueryNode.pathOrdering().sortedCopy(node.getDescendants())) {
                processing.add(desc);
            }
            if (!processing.isEmpty()) {
                processing.poll().accept(this);
            }
            return queries;
        }

        @Override
        public List<QueryNode> visit(TerminalNode node) {
            queries.add(node);
            if (!processing.isEmpty()) {
                processing.poll().accept(this);
            }
            return queries;
        }
    }
    
    public static final Matcher<QueryNode> intermediateNode(final Matcher<? super IntermediateNode> matcher) {
        return new TypeSafeDiagnosingMatcher<NodeSet.QueryNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendDescriptionOf(matcher);
            }

            @Override
            protected boolean matchesSafely(QueryNode item, final Description mismatchDescription) {
                return item.accept(new QueryNodeVisitor<Boolean>() {
                    @Override
                    public Boolean visit(IntermediateNode node) {
                        if (!matcher.matches(node)) {
                            matcher.describeMismatch(node, mismatchDescription);
                            return false;
                        }
                        return true;
                    }
                    
                    @Override
                    public Boolean visit(TerminalNode node) {
                        mismatchDescription.appendText("got terminal node");
                        return false;
                    }
                    
                });
            }
        };
    }

    public static final Matcher<QueryNode> terminalNode(final Matcher<? super TerminalNode> matcher) {
        return new TypeSafeDiagnosingMatcher<NodeSet.QueryNode>() {

            @Override
            public void describeTo(Description description) {
                description.appendDescriptionOf(matcher);
            }

            @Override
            protected boolean matchesSafely(QueryNode item, final Description mismatchDescription) {
                return item.accept(new QueryNodeVisitor<Boolean>() {
                    @Override
                    public Boolean visit(TerminalNode node) {
                        if (!matcher.matches(node)) {
                            matcher.describeMismatch(node, mismatchDescription);
                            return false;
                        }
                        return true;
                    }
                    
                    @Override
                    public Boolean visit(IntermediateNode node) {
                        mismatchDescription.appendText("got intermediate node");
                        return false;
                    }
                    
                });
            }
        };
    }

    private <T> Matcher<TerminalNode> query(AttributeQuery<T> attributeQuery) {
        return new FeatureMatcher<TerminalNode, AttributeQuery<T>>(is(attributeQuery),
            "terminal node with query", "query") {
                @Override
                @SuppressWarnings("unchecked")
                protected AttributeQuery<T> featureValueOf(TerminalNode actual) {
                    return (AttributeQuery<T>)actual.query;
                }
        };
    }

    private Matcher<IntermediateNode> children(List<QueryNode> subList) {
        return new FeatureMatcher<IntermediateNode, List<QueryNode>>(is(subList), 
            "intermediate node with descendants", "descendants") {

            @Override
            protected List<QueryNode> featureValueOf(IntermediateNode actual) {
                return QueryNode.pathOrdering().immutableSortedCopy(actual.getDescendants());
            }
        };
    }

    private Matcher<QueryNode> path(String... segments) {
        List<String> segs = ImmutableList.copyOf(segments);
        return new FeatureMatcher<NodeSet.QueryNode, List<String>>(is(segs),
            "node with path", "path") {

            @Override
            protected List<String> featureValueOf(QueryNode actual) {
                return actual.getPathSegments();
            }
        };
    }
 
    
//    private SearchHits queryHits(AtomicQuerySet query) throws Exception {
//        return esClient.client()
//            .prepareSearch()
//            .setQuery(print(builder.buildQuery(query)))
//            .execute().get().getHits();
//    }
//
//    private Map<String, Object> print(Map<String, Object> map) {
//        System.out.println(map);
//        return map;
//    }
//
//    private ActionFuture<IndexResponse> index(Node esClient, String index, String type,
//                       String id, ImmutableMap<String,Object> source) {
//        return esClient.client().index(Requests.indexRequest()
//            .index(INDEX)
//            .type(TYPE)
//            .id(id).source(
//                source
//            )
//        );
//    }
//
//    private ActionFuture<PutMappingResponse> putMapping(Node client, String index, String type, XContentBuilder mapping) {
//        return esClient.client()
//            .admin()
//            .indices()
//            .putMapping(Requests
//                .putMappingRequest(INDEX)
//                .type(TYPE)
//                .source(mapping)
//            );
//    }
//
//    private ActionFuture<CreateIndexResponse> createIndex(Node client, String index) {
//        return client.client()
//            .admin()
//            .indices()
//            .create(Requests.createIndexRequest(index));
//    }
}
