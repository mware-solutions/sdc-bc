package com.mware.stage.lib;

import com.google.inject.Inject;
import com.mware.core.bootstrap.BcBootstrap;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.Schema;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.util.ConfigurationUtils;

import java.io.File;
import java.util.Collections;
import java.util.Map;

public class BigConnectSystem {
    private Configuration configuration;
    private VisibilityTranslator visibilityTranslator;
    private Graph graph;
    private SchemaRepository ontologyRepository;
    private WorkQueueRepository workQueueRepository;
    private User user;
    private LockRepository lockRepository;
    private UserRepository userRepository;
    private AuthorizationRepository authorizationRepository;
    private SimpleOrmSession simpleOrmSession;
    private PrivilegeRepository privilegeRepository;
    private Authorizations authorizations;
    private WorkspaceRepository workspaceRepository;

    private static BigConnectSystem _INSTANCE = new BigConnectSystem();
    public static synchronized BigConnectSystem getInstance() {
        return _INSTANCE;
    }

    private BigConnectSystem() {}

    public void init(String configDir) throws Exception {
        InjectHelper.inject(this, BcBootstrap.bootstrapModuleMaker(getConfiguration(configDir)), getConfiguration(configDir));
    }

    protected Configuration getConfiguration(String configDir) throws Exception {
        if (configuration == null) {
            Map config = ConfigurationUtils.loadConfig(Collections.singletonList(
                    configDir + File.separator + "bc.properties"
            ), "");
            configuration = ConfigurationLoader.load(HashMapConfigurationLoader.class, config);
        }
        return configuration;
    }

    public Authorizations getAuthorizations() {
        if (this.authorizations == null) {
            this.authorizations = getAuthorizationRepository().getGraphAuthorizations(getSystemUser());
        }
        return this.authorizations;
    }

    public Schema getOntology(String workspaceId) {
        return ontologyRepository.getOntology(workspaceId);
    }

    public User getSystemUser() {
        if (this.user == null) {
            this.user = userRepository.getSystemUser();
        }
        return this.user;
    }


    @Inject
    public void setLockRepository(LockRepository lockRepository) {
        this.lockRepository = lockRepository;
    }

    @Inject
    public final void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Inject
    public void setAuthorizationRepository(AuthorizationRepository authorizationRepository) {
        this.authorizationRepository = authorizationRepository;
    }

    @Inject
    public void setPrivilegeRepository(PrivilegeRepository privilegeRepository) {
        this.privilegeRepository = privilegeRepository;
    }

    @Inject
    public void setWorkspaceRepository(WorkspaceRepository workspaceRepository) {
        this.workspaceRepository = workspaceRepository;
    }

    @Inject
    public final void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public final void setWorkQueueRepository(WorkQueueRepository workQueueRepository) {
        this.workQueueRepository = workQueueRepository;
    }

    @Inject
    public final void setOntologyRepository(SchemaRepository ontologyRepository) {
        this.ontologyRepository = ontologyRepository;
    }

    @Inject
    public final void setSimpleOrmSession(SimpleOrmSession simpleOrmSession) {
        this.simpleOrmSession = simpleOrmSession;
    }

    public SimpleOrmSession getSimpleOrmSession() {
        return simpleOrmSession;
    }

    public Graph getGraph() {
        return graph;
    }

    public WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public UserRepository getUserRepository() {
        return userRepository;
    }

    public AuthorizationRepository getAuthorizationRepository() {
        return authorizationRepository;
    }

    public PrivilegeRepository getPrivilegeRepository() {
        return privilegeRepository;
    }

    public VisibilityTranslator getVisibilityTranslator() {
        return visibilityTranslator;
    }

    public WorkspaceRepository getWorkspaceRepository() {
        return workspaceRepository;
    }
    @Inject
    public void setVisibilityTranslator(VisibilityTranslator visibilityTranslator) {
        this.visibilityTranslator = visibilityTranslator;
    }

    public void shutDown() {
        if (getGraph() != null) {
            getGraph().shutdown();
        }
    }

    public static String extractWorkspaceId(String dispayTitleWithId) {
        if(dispayTitleWithId.contains(SchemaRepository.PUBLIC)) {
            return SchemaRepository.PUBLIC;
        } else {
            int firstP = dispayTitleWithId.lastIndexOf('(');
            int lastP = dispayTitleWithId.lastIndexOf(')');
            return dispayTitleWithId.substring(firstP, lastP);
        }
    }
}
