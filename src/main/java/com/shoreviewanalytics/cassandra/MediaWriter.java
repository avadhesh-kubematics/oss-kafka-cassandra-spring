package com.shoreviewanalytics.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.shoreviewanalytics.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MediaWriter {

    private CqlSession session;

    private static Logger logger = LoggerFactory.getLogger(MediaWriter.class);

    public MediaWriter() {
    }

    public void WriteToCassandra(JsonNode record, CqlSession session) {

            session.execute("INSERT INTO KAFKA_EXAMPLES.VIDEOS_BY_TITLE_YEAR(TITLE,ADDED_YEAR,ADDED_DATE,DESCRIPTION,USER_ID,VIDEO_ID) " + "" +
                    "VALUES('" + record.get("title").asText() + "'," + record.get("added_year").asText() + ",'" + record.get("added_date").asText() + "','" + record.get("description").asText() + "'," +
                    "" + record.get("userid").asText() + "," + record.get("videoid").asText() + ");");

    }


    public CqlSession cqlSession(String node, Integer port, String datacenter, String username, String password) throws Exception {

        try {
            dbConnector connector = new dbConnector();
            connector.connect(node,port,datacenter,username,password);
            session = connector.getSession();

        } catch (NullPointerException ex) {
            logger.info("Handle for NullPointerException... " +
                    ex.getClass() + " " + ex.getStackTrace());
        }

        return session;
    }


}
