package com.lynne;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.*;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Filter;
import java.util.regex.Pattern;

/**
 * Created by Lynne on 2016-11-15.
 */
@CrossOrigin
@RestController
@RequestMapping("/asset/1")
public class RestControlloer {

    private final ReentrantLock lock = new ReentrantLock();
    private MongoClient client;
    private MongoDatabase db;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    public ResponseEntity<String> get() {
        this.ensureDBConnection();
        MongoCollection<BsonDocument> coll = this.db.getCollection("listings", BsonDocument.class);
        BsonDocument doc = null;
        FindIterable<BsonDocument> itr = coll.find();
        doc = itr.first();
        return new ResponseEntity<String>(doc.toJson(), HttpStatus.OK);

    }

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public ResponseEntity<String> getAllAssets() {
        this.ensureDBConnection();
        MongoCollection<BsonDocument> coll = this.db.getCollection("listings", BsonDocument.class);
        BsonDocument root = new BsonDocument();
        List<BsonDocument> list = coll.find().into(new ArrayList<BsonDocument>());
        root.append("data", new BsonArray(list));
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<String> getAsset(@PathVariable String id) {
        this.ensureDBConnection();

        if (StringUtils.isEmpty(id)) {
            return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
        }

        MongoCollection<BsonDocument> coll = this.db.getCollection("listings", BsonDocument.class);

        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        Bson filter = Filters.eq("_id", new ObjectId(id));

        BsonDocument x = coll.find(filter).first();

        if (x == null) {
            return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<String>(x.toJson(), HttpStatus.OK);

    }

    @RequestMapping(value = "/like/isin", method = RequestMethod.GET)
    public ResponseEntity<String> getAssetByIsinPrefix(@RequestParam String prefix) {
        logger.info("get Asset By Isin prefix: " + prefix);

        this.ensureDBConnection();
        if (StringUtils.isEmpty(prefix)) {
            return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
        }

        MongoCollection<BsonDocument> coll = this.db.getCollection("listings", BsonDocument.class);
        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        BsonDocument root = new BsonDocument();

        Bson filter = Filters.regex("isin", "^" + prefix + ".*");

        List<BsonDocument> list = coll.find(filter).into(new ArrayList<BsonDocument>());

        if (list.size() == 0) {
            return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
        }
        root.append("data", new BsonArray(list));
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);

    }

    @RequestMapping(value = "/currencyCount", method = RequestMethod.GET)
    public ResponseEntity<String> getAssetCurrencyCount() {

        this.ensureDBConnection();

        MongoCollection<BsonDocument> coll = this.db.getCollection("listings", BsonDocument.class);
        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        List<BsonDocument> list = new ArrayList<BsonDocument>();

        BsonDocument root = new BsonDocument();

        BsonDocument group = new BsonDocument();
        group.append("count", new BsonDocument("$sum", new BsonInt64(1)));
        group.append("_id", new BsonString("$currency"));

        Bson sort = Sorts.ascending("count");

        for (BsonDocument x : coll.aggregate(Arrays.asList(new BsonDocument("$group", group),
                new BsonDocument("$sort", new BsonDocument("count", new BsonInt64(-1)))))) {
            list.add(x);
        }

        if (list.size() == 0) {
            return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
        }
        root.append("data", new BsonArray(list));
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);

    }

    @RequestMapping(value = "/like/name", method = RequestMethod.GET)
    public ResponseEntity<String> getAssetByName(@RequestParam String name) {
        logger.info("get Asset By Name: " + name);

        this.ensureDBConnection();
        if (StringUtils.isEmpty(name)) {
            return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
        }

        MongoCollection<BsonDocument> coll = this.db.getCollection("listings", BsonDocument.class);
        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        BsonDocument root = new BsonDocument();

        Bson filter = Filters.regex("name", "^.*" + name + ".*$");

        List<BsonDocument> list = coll.find(filter).into(new ArrayList<BsonDocument>());

        if (list.size() == 0) {
            return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
        }
        root.append("data", new BsonArray(list));
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);

    }


    private void ensureDBConnection() {
        this.lock.lock();
        try {
            if (this.client == null) {
                this.client = new MongoClient("localhost", 27017);
                this.db = this.client.getDatabase("test");
            }
        } finally {
            this.lock.unlock();
        }
    }


}
