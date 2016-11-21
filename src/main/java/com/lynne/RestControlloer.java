package com.lynne;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.print.Doc;
import javax.websocket.server.PathParam;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
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
        MongoCollection<Document> coll = this.db.getCollection("listings");
        Document doc = null;
        FindIterable<Document> itr = coll.find();
        doc = itr.first();
        return new ResponseEntity<String>(doc.toJson(), HttpStatus.OK);

    }

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public ResponseEntity<String> getAllAssets() {
        this.ensureDBConnection();
        MongoCollection<Document> coll = this.db.getCollection("listings");
        Document root = new Document();
        List<Document> list = new ArrayList<Document>();
        for (Document x : coll.find()) {
            list.add(x);
        }
        root.append("data", list);
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseEntity<String> getAsset(@PathVariable String id) {
        this.ensureDBConnection();

        if (StringUtils.isEmpty(id)) {
            return new ResponseEntity<String>(HttpStatus.BAD_REQUEST);
        }

        MongoCollection<Document> coll = this.db.getCollection("listings");

        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        Document criteria = new Document();
        criteria.append("_id", new ObjectId(id));

        logger.info("criteria: " + criteria.toJson());
        Document x = coll.find(criteria).first();
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

        MongoCollection<Document> coll = this.db.getCollection("listings");
        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        Document root = new Document();
        List<Document> list = new ArrayList<Document>();

        Document regQuery = new Document();
        regQuery.append("$regex", "^" + Pattern.quote(prefix) + ".*");

        //TODO
        Filters.regex("isin", "^" + prefix + ".*");

        Document criteria = new Document();
        criteria.append("isin", regQuery);

        logger.info("criteria: " + criteria.toJson());
        for (Document x : coll.find(criteria)) {
            list.add(x);
        }

        if (list.size() == 0) {
            return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
        }
        root.append("data", list);
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);

    }

    @RequestMapping(value = "/currencyCount", method = RequestMethod.GET)
    public ResponseEntity<String> getAssetCurrencyCount() {

        this.ensureDBConnection();

        MongoCollection<Document> coll = this.db.getCollection("listings");
        if (coll == null) {
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        Document root = new Document();
        List<Document> list = new ArrayList<Document>();

        Document group = new Document();
        group.append("count", new Document("$sum", 1));
        group.append("_id", "$currency");

        for (Document x : coll.aggregate(Arrays.asList(new Document("$group", group),
                new Document("$sort", new Document("count", -1))))) {
            list.add(x);
        }
        if (list.size() == 0) {
            return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
        }
        root.append("data", list);
        return new ResponseEntity<String>(root.toJson(), HttpStatus.OK);

    }

    //TODO find by name


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
