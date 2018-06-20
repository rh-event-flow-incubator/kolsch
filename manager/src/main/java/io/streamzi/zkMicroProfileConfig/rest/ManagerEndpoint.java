package io.streamzi.zkMicroProfileConfig.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * JAX-RS endpoint for CRUD of ZooKeeper configurations.
 */
@Path("/manager")
@ApplicationScoped
public class ManagerEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(ManagerEndpoint.class);

    @Inject
    @ConfigProperty(name = "io.streamzi.zk.zkUrl", defaultValue = "localhost:2181")
    private String zkUrl;

    @Path("/upload")
    @POST
    @Consumes("multipart/form-data")
    public Response uploadFile(final MultipartFormDataInput input) {

        logger.trace("Receiving updated mapping.");

        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
        List<InputPart> inputParts = uploadForm.get("file");

        for (InputPart inputPart : inputParts) {

            try {
                final InputStream inputStream = inputPart.getBody(InputStream.class, null);

                //New configuration
                final KolschConfig config = new ObjectMapper().readValue(inputStream, KolschConfig.class);

                logger.info("new config = " + config);

                //Check for starting slashes
                if (!config.getApplicationId().startsWith("/")) {
                    config.setApplicationId("/" + config.getApplicationId());
                }

                //Check for starting slashes on the keys
                final Map<String, String> temp = new HashMap<>();
                for (Iterator<String> iterator = config.keySet().iterator(); iterator.hasNext(); ) {
                    String key = iterator.next();
                    if (!key.startsWith("/")) {

                        temp.put("/" + key, config.get(key));
                        iterator.remove();
                    }
                }
                config.getRows().putAll(temp);


                final CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, new ExponentialBackoffRetry(1000, 3));
                client.start();

                //remove keys that no longer exist in this config
                deleteNonExistingKeys(client, config, config.getApplicationId());

                //update any keys with new values
                for (String key : config.keySet()) {
                    try {

                        final String path = config.getApplicationId() + key;

                        //create znode if it doesn't exist
                        Stat stat = client.checkExists().forPath(path);
                        if (stat == null) {
                            client.createContainers(path);
                        }

                        //update the value if changed
                        final String oldValue = new String(client.getData().forPath(path));
                        final String newValue = config.get(key);

                        if (!oldValue.equals(newValue)) {
                            logger.info("Updating: " + path);
                            client.setData().forPath(path, config.get(key).getBytes());
                        } else {
                            logger.info("Not updating: " + path);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                client.close();

            } catch (IOException e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
            }
        }
        return Response.status(Response.Status.OK).build();
    }

    private void deleteNonExistingKeys(final CuratorFramework client, final KolschConfig config, final String root) {
        logger.trace("Looking for nodes to delete: " + root);

        try {
            final Stat stat = client.checkExists().forPath(root);
            if (stat != null) {
                final List<String> children = client.getChildren().forPath(root);

                //Leaf node
                if (children.size() == 0) {

                    //Delete if not in the config
                    if (!config.keySet().contains(root.substring(config.getApplicationId().length()))) {
                        logger.info("Deleting: " + root);
                        client.delete().forPath(root);
                    }

                } else {

                    //recurse
                    for (String child : children) {
                        final String fullPath = root + "/" + child;
                        deleteNonExistingKeys(client, config, fullPath);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}