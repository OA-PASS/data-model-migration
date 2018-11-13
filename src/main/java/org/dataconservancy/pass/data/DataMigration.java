package org.dataconservancy.pass.data;
import java.io.FileOutputStream;

import java.net.URI;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import org.dataconservancy.pass.client.PassClient;
import org.dataconservancy.pass.client.PassClientFactory;
import org.dataconservancy.pass.client.SubmissionStatusService;
import org.dataconservancy.pass.client.fedora.FedoraConfig;
import org.dataconservancy.pass.model.Funder;
import org.dataconservancy.pass.model.Grant;
import org.dataconservancy.pass.model.Repository;
import org.dataconservancy.pass.model.Submission;
import org.dataconservancy.pass.model.Submission.Source;
import org.dataconservancy.pass.model.SubmissionEvent;
import org.dataconservancy.pass.model.SubmissionEvent.EventType;
import org.dataconservancy.pass.model.SubmissionEvent.PerformerRole;
import org.dataconservancy.pass.model.User;
import org.dataconservancy.pass.model.support.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dataconservancy.pass.data.RepositoryMigrator.migrateRepositories;

/*
 * Copyright 2018 Johns Hopkins University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Data migration code
 * @author Karen Hanson
 */
public class DataMigration {

    private static final Logger LOG = LoggerFactory.getLogger(DataMigration.class);

    private static final String DOMAIN = "johnshopkins.edu";
    private static final String EMPLOYEE_ID_TYPE = "employeeid";
    private static final String HOPKINS_ID_TYPE = "hopkinsid";
    private static final String JHED_ID_TYPE = "jhed";
    private static final String GRANT_ID_TYPE = "grant";
    private static final String FUNDER_ID_TYPE = "funder";

    private static int successfulSubmissions = 0;
    private static int unsuccessfulSubmissions = 0;
    private static int createdSubmissionEvents = 0;

    private static int successfulObjects = 0;
    private static int unsuccessfulObjects = 0;
    private static int skippedObjects = 0;

    private static PassClient client;
    private static org.dataconservancy.pass.v2_3.client.PassClient oldClient;
    private static SubmissionStatusService statusService;
    
    private final static String PASS_BASE_URL = "http://localhost:8080/fcrepo/rest/";
    private final static String PASS_ELASTICSEARCH_URL = "http://localhost:9200/pass/";
    private final static String PASS_FEDORA_USER = "fedoraAdmin";
    private final static String PASS_FEDORA_PASSWORD = "moo";
    private final static String PASS_SEARCH_LIMIT = "10000";
    
    static {
        //Java client defaults will work in pass-docker environment, can override these by uncommenting hereafter
        //System.setProperty("pass.fedora.baseurl", PASS_BASE_URL);
        //System.setProperty("pass.elasticsearch.url", PASS_ELASTICSEARCH_URL);
        //System.setProperty("pass.fedora.user", PASS_FEDORA_USER);
        //System.setProperty("pass.fedora.password", PASS_FEDORA_PASSWORD);
        System.setProperty("pass.elasticsearch.limit", PASS_SEARCH_LIMIT);
    }

    static final java.io.File dumpDir = new java.io.File("dump-" + new Date().getTime());

    static final java.io.File deletedDir = new java.io.File(dumpDir, "deleted");

    static final java.io.File editedDir = new java.io.File(dumpDir, "edited");
    
    static CloseableHttpClient http;

    public static void main(String[] args) {
        
        try {

            client = PassClientFactory.getPassClient(true);
            oldClient = org.dataconservancy.pass.v2_3.client.PassClientFactory.getPassClient();
            statusService = new SubmissionStatusService();
            //include this block if you want to dump a copy of each deleted or edited record into a local folder.
            //for an example of how this is used, see delete consumer below
            /*
            http = getAuthClient();
            editedDir.mkdirs();
            deletedDir.mkdirs();
            System.out.println("Dumping deleted resources in " + deletedDir.getAbsolutePath());
            System.out.println("Dumping resources prior to editing in " + editedDir.getAbsolutePath());
            */
            
            migrateSubmissionModel();
            migrateRepositories(client);
            migrateUsers();
            migrateGrants();
            migrateFunders();
                        
        } catch (Exception ex)  {
            System.err.println("Update failed: " + ex.getMessage());
        }
    }    
    
    private static void migrateSubmissionModel() {
    	successfulSubmissions=0;
    	unsuccessfulSubmissions=0;
    	createdSubmissionEvents=0;
    	int recordsProcessed = client.processAllEntities(uri -> migrateSubmission(uri), Submission.class);
        
        LOG.info("********************************************************");
        LOG.info("Submission crawled: {}", recordsProcessed);
        LOG.info("Submissions successfully updated: {}", successfulSubmissions);
        LOG.info("Submissions with failed update: {}", unsuccessfulSubmissions);
        LOG.info("Submission Events created: {}", createdSubmissionEvents);
        LOG.info("********************************************************");
    }
    private static void migrateUsers() {
    	successfulObjects = 0;
        unsuccessfulObjects = 0;
        skippedObjects = 0;
        int recordsProcessed =  client.processAllEntities(uri -> migrateUser(uri), User.class);

        LOG.info("********************************************************");
        LOG.info("Users crawled: {}", recordsProcessed);
        LOG.info("Users successfully updated: {}", successfulObjects);
        LOG.info("Users with failed update: {}", unsuccessfulObjects);
        LOG.info("Users skipped: {}", skippedObjects);
        LOG.info("********************************************************");
    }

    private static void migrateGrants() {
        successfulObjects = 0;
        unsuccessfulObjects = 0;
        skippedObjects = 0;
        int recordsProcessed = client.processAllEntities(uri -> migrateGrant(uri), Grant.class);

        LOG.info("********************************************************");
        LOG.info("Grants crawled: {}", recordsProcessed);
        LOG.info("Grants successfully updated: {}", successfulObjects);
        LOG.info("Grants with failed update: {}", unsuccessfulObjects);
        LOG.info("Grants skipped: {}", skippedObjects);
        LOG.info("********************************************************");
    }

    private static void migrateFunders() {
        successfulObjects = 0;
        unsuccessfulObjects = 0;
        skippedObjects = 0;
        int recordsProcessed = client.processAllEntities(uri -> migrateFunder(uri), Funder.class);

        LOG.info("********************************************************");
        LOG.info("Funders crawled: {}", recordsProcessed);
        LOG.info("Funders successfully updated: {}", successfulObjects);
        LOG.info("Funders with failed update: {}", unsuccessfulObjects);
        LOG.info("Funders skipped: {}", skippedObjects);
        LOG.info("********************************************************");
    }


    private static void migrateSubmission(URI uri) {
        
        try {
            Submission newSubmission = client.readResource(uri, Submission.class);
            org.dataconservancy.pass.v2_3.model.Submission origSubmission = oldClient.readResource(uri, org.dataconservancy.pass.v2_3.model.Submission.class);
            URI submitter = origSubmission.getUser();
            
            //If there is already a value in submitter and submissionStatus, don't need to redo
            if (newSubmission.getSubmitter()==null
                    || newSubmission.getSubmissionStatus()==null
                    || submitter!=null) {
                newSubmission.setSubmitter(submitter);
                newSubmission.setSubmissionStatus(statusService.calculateSubmissionStatus(newSubmission));
                client.updateResource(newSubmission);
                LOG.info("Submission:{} was updated. Submitter:{}, Status:{}", 
                         uri, newSubmission.getSubmitter(), newSubmission.getSubmissionStatus());
            } 
            
            // create event if need one
            if (newSubmission.getSource().equals(Source.PASS)
                    && newSubmission.getSubmitted()) {
                // will check if one already exists, this allows for re-run
                Map<String, Object> submEventSearch = new HashMap<String, Object>();
                submEventSearch.put("eventType", "submitted");
                submEventSearch.put("submission", uri);
                Set<URI> events = client.findAllByAttributes(SubmissionEvent.class, submEventSearch);
                if (events.size()==0) {
                    SubmissionEvent event = new SubmissionEvent();
                    event.setSubmission(uri);
                    event.setPerformedBy(submitter);
                    event.setEventType(EventType.SUBMITTED);
                    event.setPerformerRole(PerformerRole.SUBMITTER);
                    event.setPerformedDate(newSubmission.getSubmittedDate());
                    URI eventUri = client.createResource(event);
                    LOG.info("SubmissionEvent:{} was created for Submission {}", eventUri, uri);
                    createdSubmissionEvents = createdSubmissionEvents+1;
                }
            }
            successfulSubmissions = successfulSubmissions+1;
            
        } catch (Exception ex) {
            LOG.error("Could not update Submission {}. Error mesage: {}", uri, ex.getMessage());
            unsuccessfulSubmissions = unsuccessfulSubmissions+1;
            //continue anyway
        }
       
    }
    private static void migrateUser(URI uri) {
        try {
            User newUser = client.readResource(uri, User.class);
            org.dataconservancy.pass.v2_3.model.User origUser = oldClient.readResource(uri, org.dataconservancy.pass.v2_3.model.User.class);
            boolean update = false;
            List<String> ids = new ArrayList<>();

            if (newUser.getLocatorIds().size() == 0) {
                if (origUser.getLocalKey() != null) {
                    ids.add(new Identifier(DOMAIN, EMPLOYEE_ID_TYPE, origUser.getLocalKey()).serialize());
                }
                if (origUser.getInstitutionalId() != null) {
                    String instId = origUser.getInstitutionalId().toLowerCase();
                    ids.add(new Identifier(DOMAIN, JHED_ID_TYPE, instId).serialize());
                }
                if (ids.size() > 0) {
                    newUser.setLocatorIds(ids);
                    update = true;
                }
            }

            if (newUser.getEmail() == null) {
                if (origUser.getEmail() != null) {
                    newUser.setEmail(origUser.getEmail());
                    update = true;
                }
            }

            if (newUser.getDisplayName() == null) {
                if (origUser.getFirstName() != null && origUser.getLastName() != null) {
                    newUser.setDisplayName(String.join(" ", origUser.getFirstName(), origUser.getLastName()));
                    update = true;
                }
            }

            if (update) {
                client.updateResource(newUser);
                successfulObjects++;
            } else {
                skippedObjects++;
            }
        } catch (Exception ex) {
            LOG.error("Could not update User {}. Error message: {}", uri, ex.getMessage());
            unsuccessfulObjects++;
        }
    }

    private static void migrateGrant(URI uri) {
        try {
            Grant grant = client.readResource(uri, Grant.class);
            if (!grant.getLocalKey().startsWith(DOMAIN)) {
                grant.setLocalKey(new Identifier(DOMAIN, GRANT_ID_TYPE, grant.getLocalKey()).serialize());
                client.updateResource(grant);
                successfulObjects++;
            } else {
                skippedObjects++;
            }
        } catch (Exception ex) {
            LOG.error("Could not update Grant {}. Error message: {}", uri, ex.getMessage());
            unsuccessfulObjects++;
        }
    }

    private static void migrateFunder(URI uri) {
        try {
            Funder funder = client.readResource(uri, Funder.class);
            if (!funder.getLocalKey().startsWith(DOMAIN)) {
                funder.setLocalKey(new Identifier(DOMAIN, FUNDER_ID_TYPE, funder.getLocalKey()).serialize());
                client.updateResource(funder);
                successfulObjects++;
            } else {
                skippedObjects++;
            }
        } catch (Exception ex) {
            LOG.error("Could not update Funder {}. Error message: {}", uri, ex.getMessage());
            unsuccessfulObjects++;
        }
    }
 
    private static Consumer<URI> delete = (id) -> {
        dump(deletedDir, id);
        client.deleteResource(id);
        System.out.println("Deleted resource with URI " + id.toString());
    };
    
    // This causes us to do another fetch of the resource content, but oh well
    private static void dump(java.io.File dir, URI uri) {
        final String path = uri.getPath();

        final java.io.File dumpfile = new java.io.File(dir, path + ".nt");
        dumpfile.getParentFile().mkdirs();

        final HttpGet get = new HttpGet(uri);
        get.setHeader("Accept", "application/n-triples");

        try (FileOutputStream out = new FileOutputStream(dumpfile);
                CloseableHttpResponse response = http.execute(get)) {

            response.getEntity().writeTo(out);

        } catch (final Exception e) {
            throw new RuntimeException("Error dumping contents of " + uri, e);
        }

    }

    static CloseableHttpClient getAuthClient() {
        final CredentialsProvider provider = new BasicCredentialsProvider();
        final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(FedoraConfig.getUserName(),
                FedoraConfig.getPassword());
        provider.setCredentials(AuthScope.ANY, credentials);

        return HttpClientBuilder.create()
                .setDefaultCredentialsProvider(provider)
                .build();
    }

    
}
