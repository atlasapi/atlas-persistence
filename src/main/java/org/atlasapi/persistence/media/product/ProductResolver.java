package org.atlasapi.persistence.media.product;

import com.google.common.base.Optional;
import org.atlasapi.media.product.Product;

public interface ProductResolver extends ProductLister {

    Optional<Product> productForId(long id);
        
    Iterable<Product> productsForContent(String canonicalUri);
    
}
