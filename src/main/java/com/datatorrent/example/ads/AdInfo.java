package com.datatorrent.example.ads;

/**
 * This represent an event for an application.
 */
public class AdInfo {
    /**
     * Id of an impression, it is a globally unique identifier for an impression.
     */
    private String id;
    /**
     * Timestamp when impression occurred.
     */
    private long timeStamp;

    /**
     * Id of the publisher. (name of top level entity)
     */
    private String publisher;

    /**
     * Domain on publisher, where ad was shown.
     */
    private String domain;

    /**
     * Page url where Ad was shown
     */
    private String url;

    /**
     * Location on the page
     */
    private int adLocation;

    /**
     * Advertiser who is showing the ad.
     */
    private String advertiser;

    /**
     * Id of an campain.
     */
    private String campainId;

    /**
     * Actual id of an ad which was shown.
     */
    private long assetId;

    /**
     * bidding cost associated with impression.
     */
    private double ecpm;

    /**
     * Whether the impression was clicked.
     */
    private boolean clicked;

    /**
     * User specific information
     */
    private String user_hash;

    /**
     * OS used by client.
     */
    private String os;

    /**
     * browser type type.
     */
    private String client_id;

    /**
     * IP address of client.
     */
    private long ip;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getAdLocation() {
        return adLocation;
    }

    public void setAdLocation(int adLocation) {
        this.adLocation = adLocation;
    }

    public String getAdvertiser() {
        return advertiser;
    }

    public void setAdvertiser(String advertiser) {
        this.advertiser = advertiser;
    }

    public String getCampainId() {
        return campainId;
    }

    public void setCampainId(String campainId) {
        this.campainId = campainId;
    }

    public long getAssetId() {
        return assetId;
    }

    public void setAssetId(long assetId) {
        this.assetId = assetId;
    }

    public double getEcpm() {
        return ecpm;
    }

    public void setEcpm(double ecpm) {
        this.ecpm = ecpm;
    }

    public boolean isClicked() {
        return clicked;
    }

    public void setClicked(boolean clicked) {
        this.clicked = clicked;
    }

    public String getUser_hash() {
        return user_hash;
    }

    public void setUser_hash(String user_hash) {
        this.user_hash = user_hash;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getClient_id() {
        return client_id;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public long getIp() {
        return ip;
    }

    public void setIp(long ip) {
        this.ip = ip;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

  @Override
  public String toString()
  {
    final StringBuffer sb = new StringBuffer("AdInfo{");
    sb.append("id='").append(id).append('\'');
    sb.append(", timeStamp=").append(timeStamp);
    sb.append(", publisher='").append(publisher).append('\'');
    sb.append(", domain='").append(domain).append('\'');
    sb.append(", url='").append(url).append('\'');
    sb.append(", adLocation=").append(adLocation);
    sb.append(", advertiser='").append(advertiser).append('\'');
    sb.append(", campainId='").append(campainId).append('\'');
    sb.append(", assetId=").append(assetId);
    sb.append(", ecpm=").append(ecpm);
    sb.append(", clicked=").append(clicked);
    sb.append(", user_hash='").append(user_hash).append('\'');
    sb.append(", os='").append(os).append('\'');
    sb.append(", client_id='").append(client_id).append('\'');
    sb.append(", ip=").append(ip);
    sb.append('}');
    return sb.toString();
  }
}
