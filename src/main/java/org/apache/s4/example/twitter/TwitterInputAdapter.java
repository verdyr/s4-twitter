/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.example.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.s4.base.Event;
import org.apache.s4.core.adapter.AdapterApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.StallWarning;

// SB --->>>
import twitter4j.GeoLocation;
import twitter4j.Place;
// <<<--- SB

public class TwitterInputAdapter extends AdapterApp {

    int countReceivedMsgs = 0; //
    private static Logger logger = LoggerFactory.getLogger(TwitterInputAdapter.class);

    public TwitterInputAdapter() {
    }

    private LinkedBlockingQueue<Status> messageQueue = new LinkedBlockingQueue<Status>();

    protected ServerSocket serverSocket;

    private Thread t;

    @Override
    protected void onClose() {
    }

    @Override
    protected void onInit() {
        super.onInit();
        t = new Thread(new Dequeuer());
    }

    public void connectAndRead() throws Exception {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        Properties twitterProperties = new Properties();

// !!!!!!!!! AWS vs In-House setup !!!!!!!!!!!
//        File twitter4jPropsFile = new File(System.getProperty("user.home") + "/twitter4j.properties");
        File twitter4jPropsFile = new File("/home/ec2-user" + "/twitter4j.properties");

        if (!twitter4jPropsFile.exists()) {
            logger.error(
                    "Cannot find twitter4j.properties file in this location :[{}]. Make sure it is available at this place and includes user/password credentials",
                    twitter4jPropsFile.getAbsolutePath());
            return;
        }
        twitterProperties.load(new FileInputStream(twitter4jPropsFile));

	System.setProperty("https.proxyHost","PROXY.ADDRESS.com"); // COMMENT OUT if no hardcoded proxy support
        System.setProperty("https.proxyPort","8080"); // COMMENT OUT if no hardcoded proxy support

	// OAuth support
        cb.setDebugEnabled(Boolean.valueOf(twitterProperties.getProperty("debug")))
//              .setUser(twitterProperties.getProperty("user"))
//		.setPassword(twitterProperties.getProperty("password"))
		.setOAuthConsumerKey(twitterProperties.getProperty("key"))
		.setOAuthConsumerSecret(twitterProperties.getProperty("secret"))
		.setOAuthAccessToken(twitterProperties.getProperty("token"))
		.setOAuthAccessTokenSecret(twitterProperties.getProperty("token_secret"));



        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener statusListener = new StatusListener() {

            @Override
            public void onException(Exception ex) {
                logger.error("error", ex);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                logger.error("error");
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                logger.error("error");
            }

            @Override
            public void onStatus(Status status) {
                messageQueue.add(status);

            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                logger.error("error");
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.error("error");
            }
        };
        twitterStream.addListener(statusListener);
        twitterStream.sample();

    }

    @Override
    protected void onStart() {
        try {
            t.start();
            connectAndRead();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class Dequeuer implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    countReceivedMsgs++;
                    logger.info("Total messages received: [{}]", countReceivedMsgs);
                    Status status = messageQueue.take();

                    Event event = new Event();

                    GeoLocation locationStatus = status.getGeoLocation();
                    double locationLat = locationStatus.getLatitude();
                    double locationLon = locationStatus.getLongitude();
                    Place placeStatus = status.getPlace();

// Extended event with Location and Place name
                    event.put("statusText", String.class, status.getText() + " " + placeStatus.getFullName() + " " + String.valueOf(locationLat) + " " + String.valueOf(locationLon) + " " + String.valueOf(countReceivedMsgs));

// Plain event w/o Location and Place name
//                  event.put("statusText", String.class, status.getText());

                    getRemoteStream().put(event);
                } catch (Exception e) {

                }
            }

        }
    }
}
