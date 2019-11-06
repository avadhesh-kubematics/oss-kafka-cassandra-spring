package com.shoreviewanalytics.kafka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.StringTokenizer;

public class Media {
    private String title;
    private String added_year;
    private String added_date;
    private String description;
    private String userid;
    private String videoid;

    public Media(){
    }
    public Media(@JsonProperty("title") String title,
                 @JsonProperty("added_year") String added_year,
                 @JsonProperty("added_date") String added_date,
                 @JsonProperty("description") String description,
                 @JsonProperty("userid") String userid,
                 @JsonProperty("videoid") String videoid) {
        this.title = title;
        this.added_year = added_year;
        this.added_date = added_date;
        this.description = description;
        this.userid = userid;
        this.videoid = videoid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAdded_year() {
        return added_year;
    }

    public void setAdded_year(String added_year) {
        this.added_year = added_year;
    }

    public String getAdded_date() {
        return added_date;
    }

    public void setAdded_date(String added_date) {
        this.added_date = added_date;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getVideoid() {
        return videoid;
    }

    public void setVideoid(String videoid) {
        this.videoid = videoid;
    }

    @Override
    public String toString() {
        return "Video [" +
                "title=" + title + ", " +
                "added_year=" + added_year + ", " +
                "added_date=" + added_date + ", " +
                "description=" + description + ", " +
                "userid=" + userid + ", " +
                "videoid=" + videoid +"]";
    }

}
