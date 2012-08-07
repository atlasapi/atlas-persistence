package org.atlasapi.persistence.media.product;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import org.atlasapi.media.product.Product;

public interface ProductStore {

    Optional<Product> productForSourceIdentified(Publisher source, String sourceIdentifier);
    
    Product store(Product product);
    
}
