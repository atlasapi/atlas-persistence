package org.atlasapi.persistence.media.product;

import com.google.common.base.Optional;
import org.atlasapi.media.product.Product;

public interface ProductResolver {

    Optional<Product> productForId(long id);
    
    Iterable<Product> products();
    
    Iterable<Product> productsForContent(String canonicalUri);
    
}
