package com.datatorrent.example.ads;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AdInfoGenerator {

    private static final Random rand = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Return list of lines are a list from resource file.
     * @param fileName
     * @return
     * @throws IOException
     */
    protected List<String> loadResource(String fileName) throws IOException
    {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(fileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        List<String> ret = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
           ret.add(line);
        }
        return ret;
    }

    private <T> T choose(List<T> lst)
    {
        int idx = rand.nextInt(lst.size());
        return lst.get(idx);
    }

    private <T> T choose(T ...lst)
    {
        int idx = rand.nextInt(lst.length);
        return lst[idx];
    }

    protected List<String> advetisers;
    protected List<String> publishersNames = new ArrayList<>();
    protected Map<String, List<String>> publishers = new HashMap<>();

    protected List<String> getArray(JSONArray arr) throws JSONException {
        List<String> ret = new ArrayList<>(arr.length());
        for (int i = 0; i < arr.length(); i++) {
            ret.add(arr.getString(i));
        }
        return ret;
    }

    public AdInfoGenerator() throws IOException, JSONException {
        advetisers = loadResource("advertisers.txt");
        List<String> publishersLines = loadResource("publishers.txt");
        for (String line : publishersLines) {
            JSONObject json = new JSONObject(line);
            String publisherName = json.getString("publisher");
            publishersNames.add(publisherName);
            publishers.put(publisherName, getArray(json.getJSONArray("domains")));
        }
    }

    public AdInfo generateRecord()
    {
        AdInfo ad = new AdInfo();
        // global information
        ad.setTimeStamp(System.currentTimeMillis());
        ad.setId(UUID.randomUUID().toString());

        //Info related to publisher
        String publisher = choose(publishersNames);
        ad.setPublisher(publisher);
        String domain = choose(publishers.get(publisher));
        ad.setDomain(domain);
        ad.setAdLocation(rand.nextInt(5) + 1);
        ad.setUrl("/page" + rand.nextInt(100) + 1);

        // info related to advertiser
        ad.setAdvertiser(choose(advetisers));
        ad.setCampainId("camp" + rand.nextInt(10));
        ad.setAssetId(rand.nextInt(20));

        // info related to users
        ad.setOs(choose("Windows", "Linux", "Android", "IOS", "MacOs"));
        ad.setClient_id(choose("Firefox", "Chrome", "IE", "IE 11", "Opera", "curl", "Safari"));
        ad.setIp(rand.nextInt());
        ad.setUser_hash(Long.toHexString(Math.abs(rand.nextLong())));

        // metrics
        ad.setEcpm(rand.nextDouble() * 5);
        ad.setClicked(rand.nextDouble() > 0.99);

        return ad;
    }

    public String toJson(AdInfo info) throws JsonProcessingException {
        return mapper.writeValueAsString(info);
    }
}
