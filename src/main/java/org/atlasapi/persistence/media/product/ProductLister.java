package org.atlasapi.persistence.media.product;

import org.atlasapi.media.product.Product;

public interface ProductLister {
    
    Iterable<Product> products();
}
