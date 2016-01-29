package org.atlasapi;

import org.atlasapi.persistence.MongoContentPersistenceModule;
import org.atlasapi.persistence.logging.AdapterLog;
import org.jmock.auto.Mock;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.ReadPreference;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes= {MongoContentPersistenceModule.class})
public class MongoContentPersistenceModuleSpringTest {

    private @InjectMocks MongoContentPersistenceModule contentPersistenceModule;

    private @Mock ReadPreference readPreference;

    private @Mock AdapterLog adapterLog;

    public void testWiring() {

    }

}
