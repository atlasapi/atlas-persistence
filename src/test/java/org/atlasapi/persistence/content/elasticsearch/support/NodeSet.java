package org.atlasapi.persistence.content.elasticsearch.support;

import java.util.Collections;
import java.util.List;

import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.persistence.content.elasticsearch.support.NodeSet.QueryNode.IntermediateNode;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public final class NodeSet {
    
    IntermediateNode root = new IntermediateNode(ImmutableList.<String>of());
    
    public NodeSet(Iterable<AttributeQuery<?>> queries) {
        for (AttributeQuery<?> attributeQuery : queries) {
            ImmutableList<String> path = attributeQuery.getAttribute().getPath();
            root.add(attributeQuery, path, 0, 0);
        }
    }

    public NodeSet() {
    }

    public NodeSet addQuery(AttributeQuery<?> query) {
        root.add(query, query.getAttribute().getPath(), 0, 0);
        return this;
    }

    @Override
    public String toString() {
        return root.toString();
    }
    
    public static abstract class QueryNode {
        
        List<String> pathSegments = ImmutableList.of();
        
        public abstract QueryNode add(AttributeQuery<?> query, List<String> path, int depth, int reach);
        public abstract String prettyPrint(int depth);

        protected List<String> commonPath(int depth, List<String> pathSegments, List<String> path) {
            int offset = depth;
            List<String> parts = Lists.newLinkedList();
            for(int i = 0; i < Math.min(pathSegments.size(),path.size()) && pathSegments.get(i).equals(path.get(i+offset)); i++) {
                parts.add(path.get(i+offset));
            }
            return parts;
        }

        public static final class TerminalNode extends QueryNode {
            
            public TerminalNode(List<String> pathParts, AttributeQuery<?> query) {
                this.pathSegments = pathParts;
                this.query = query;
            }

            AttributeQuery<?> query;
            
            @Override
            public String toString() {
                return prettyPrint(0);
            }

            @Override
            public QueryNode add(AttributeQuery<?> query, List<String> path, int pathStart, int pathEnd) {
                int newReach = 0;
                while (newReach < pathSegments.size() && this.pathSegments.get(newReach).equals(path.get(pathStart + newReach))) {
                    newReach++;
                }
                List<String> commonSegments = pathSegments.subList(0, newReach);
                this.pathSegments = pathSegments.subList(newReach,
                    pathSegments.size());
                return new IntermediateNode(commonSegments,
                    this,
                    new TerminalNode(path.subList(pathStart+newReach, path.size()), query)
                );
            }

            @Override
            public String prettyPrint(int depth) {
                return Joiner.on("").join(Collections.nCopies(depth, "\t"))
                    + pathSegments + "==>" + query;
            }

        }
        
        public static final class IntermediateNode extends QueryNode {
            List<QueryNode> descendants = Lists.newLinkedList();
            
            public IntermediateNode(List<String> pathSegments) {
                this.pathSegments = pathSegments;
            }
            public IntermediateNode(List<String> pathSegments, QueryNode left, QueryNode right) {
                this.pathSegments = pathSegments;
                this.descendants = Lists.newArrayList(left, right);
            }

            public QueryNode add(AttributeQuery<?> query, List<String> path, int pathStart, int pathEnd) {
                int newReach = 0;
                while (newReach < pathSegments.size() && this.pathSegments.get(newReach).equals(path.get(pathStart + newReach))) {
                    newReach++;
                }
                String key = path.get(pathStart + newReach);
                //List<String> commonPath = commonPath(depth-1, pathSegments, path);
                QueryNode cur = null;
                if (newReach == pathSegments.size()) {
                    cur = getParentNode(descendants, key);
                } else {
                    QueryNode.IntermediateNode pushedDown =
                        new IntermediateNode(pathSegments.subList(newReach,
                            pathSegments.size()));
                    pushedDown.descendants = this.descendants;
                    return new IntermediateNode(
                        pathSegments.subList(0, newReach),
                        pushedDown,
                        new TerminalNode(path.subList(pathStart+newReach, path.size()), query));
                }
                QueryNode nn = null;
                if (cur == null) {
//                    if (pathStart+pathEnd == pathSegments.size()) {
//                        
//                    } else {
//                        
//                    }
                    if (newReach == pathSegments.size()) {
                        List<String> pathSegments = path.subList(pathStart+newReach, path.size());
                        descendants.add(new TerminalNode(pathSegments, query));
                        return this;
                    } else {
                        QueryNode.IntermediateNode pushedDown =
                            new IntermediateNode(pathSegments.subList(newReach,
                                pathSegments.size()));
                        pushedDown.descendants = this.descendants;
                        return new IntermediateNode(
                            pathSegments.subList(0, newReach),
                            pushedDown,
                            new TerminalNode(path.subList(pathStart+newReach, path.size()), query));
                    }
                } else {
                    
//                    if (cur.pathSegments.equals(path.subList(pathStart, path.size()-pathStart))) {
                        descendants.remove(cur);
//                        int newReach = pathStart;
//                        while (cur.pathSegments.get(newReach).equals(path.get(newReach))) {
//                            newReach++;
//                        }
                        nn = cur.add(query, path, pathStart+newReach, pathEnd);
                        descendants.add(nn);
                        return this;
//                    } else {
//                        QueryNode.IntermediateNode pushedDown =
//                            new IntermediateNode(pathSegments.subList(pathStart - 1 + pathEnd,
//                                pathSegments.size()));
//                        pushedDown.descendants = this.descendants;
//                        return new IntermediateNode(
//                            pathSegments.subList(pathStart - 1, pathEnd),
//                            pushedDown,
//                            new TerminalNode(path.subList(pathStart - 1 + pathEnd, path.size()), query));
//                    }
                }
            }
            
            private QueryNode getParentNode(List<QueryNode> descs, String key) {
//                String key = path.get(depth);
                for (QueryNode desc : descs) {
                    if (desc.pathSegments.get(0).equals(key)) {
                        return desc;
                    }
                }
                return null;
            }

            public String prettyPrint(int depth) {
                String string = Joiner.on("").join(Collections.nCopies(depth, "\t"))
                    + pathSegments + "==>";
                for (QueryNode descendant : descendants) {
                    string += "\n" + descendant.prettyPrint(depth+1);
                }
                return string;
            }

            @Override
            public String toString() {
                return prettyPrint(0);
            }
            
        }

    }
}