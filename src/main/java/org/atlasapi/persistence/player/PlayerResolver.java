package org.atlasapi.persistence.player;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Player;

import com.google.common.base.Optional;


public interface PlayerResolver {
    
    Optional<Player> playerFor(long id);
    Iterable<Player> playersFor(Alias alias);
        
}
