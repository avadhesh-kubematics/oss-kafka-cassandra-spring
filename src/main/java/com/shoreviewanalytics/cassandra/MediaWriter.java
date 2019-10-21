package com.shoreviewanalytics.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.shoreviewanalytics.kafka.domain.Media;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

public class MediaWriter {

    private static Logger logger = LoggerFactory.getLogger(MediaWriter.class);
    /*
    This method is not used in the current implementation.  It is leftover for reference. Notice that
    the dbConnector object has been moved below to a standalone method called session.
     */
    public void WriteToCassandra(Vector records, CqlSession session) throws Exception {

        dbConnector connector = new dbConnector();
        connector.connect("cassandra-23daba12-shoreviewanalytics-d9c3.aivencloud.com", 12641, "aiven");
        session = connector.getSession();


        try {
            dbDDL dbDDL = new dbDDL(session);
            dbDDL.createVideoTable("KAFKA_EXAMPLES", "VIDEOS_BY_TITLE_YEAR");
        } catch (com.datastax.oss.driver.api.core.servererrors.QueryExecutionException ex) {
            logger.info(ex.getMessage());
        }


        int counter = 0;
        for (Object record : records) {
            Media media = (Media) record;
            counter = counter + 1;


            session.execute("INSERT INTO KAFKA_EXAMPLES.VIDEOS_BY_TITLE_YEAR(TITLE,ADDED_YEAR,ADDED_DATE,DESCRIPTION,USER_ID,VIDEO_ID) " + "" +
                    "VALUES('" + media.getTitle() + "'," + media.getAdded_year() + ",'" + media.getAdded_date() + "','" + media.getDescription() + "'," +
                    "" + media.getUserid() + "," + media.getVideoid() + ");");

        }
        logger.info("Total records inserted: " + counter);

        logger.info("Finished inserting records");
    }

    public void WriteToCassandra(JsonNode record, CqlSession session) {

            session.execute("INSERT INTO KAFKA_EXAMPLES.VIDEOS_BY_TITLE_YEAR(TITLE,ADDED_YEAR,ADDED_DATE,DESCRIPTION,USER_ID,VIDEO_ID) " + "" +
                    "VALUES('" + record.get("title").asText() + "'," + record.get("added_year").asText() + ",'" + record.get("added_date").asText() + "','" + record.get("description").asText() + "'," +
                    "" + record.get("userid").asText() + "," + record.get("videoid").asText() + ");");


    }

    public CqlSession cqlSession() throws Exception {
        dbConnector connector = new dbConnector();
        connector.connect("10.1.10.61", 9042, "dc1");
        CqlSession session = connector.getSession();
        return session;
    }


}
