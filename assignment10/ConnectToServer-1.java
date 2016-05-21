package org.neo4j.graphproject;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ConnectToServer
{
    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

    public static void main( String[] args ) throws URISyntaxException
    {
          checkDatabaseIsRunning();

          //sendTransactionalCypherQuery("MATCH (n) DETACH DELETE n");

//          // START SNIPPET: nodesAndProps
            //URI firstNode = createNode();
            //addProperty( firstNode, "name", "Joe Strummer" );

            // Keanu Reeves acted in the movie “John Wick”
          	URI movieNode = createNode();
          	addProperty( movieNode, "title", "John Wick" );
          	sendTransactionalCypherQuery("MATCH (n { title: 'John Wick' }) SET n:Movie RETURN n");
          	addProperty( movieNode, "year", "2014-10-24" );
          	
          	
          	// directed by Chad Stahelski and David Leitch
          	URI directorOne = createNode();
          	addProperty( directorOne, "name", "Chad Stahelski" );
          	sendTransactionalCypherQuery("MATCH (n { name: 'Chad Stahelski' }) SET n:Director RETURN n");
          	URI relationshipUri = addRelationship( directorOne, movieNode, "DIRECTS", null);
          	addMetadataToProperty(relationshipUri, "role", "Director");

          	URI directorTwo = createNode();
          	addProperty( directorTwo, "name", "David Leitch");
          	sendTransactionalCypherQuery("MATCH (n { name: 'David Leitch' }) SET n:Director RETURN n");
          	URI relationshipUriTwo = addRelationship( directorTwo, movieNode, "DIRECTS", null);
          	addMetadataToProperty(relationshipUriTwo, "role", "Director");

          	// Actor William Dafoe
        	URI actorOne = createNode();
          	addProperty( actorOne, "name", "William Dafoe");
          	sendTransactionalCypherQuery("MATCH (n { name: 'William Dafoe' }) SET n:Actor RETURN n");
          	URI relationshipUriThree = addRelationship( actorOne, movieNode, "ACTS_IN", null);
          	addMetadataToProperty(relationshipUriThree, "role", "Marcus");
          	
          	// Actor Michael Nyquist
          	URI actorTwo = createNode();
          	addProperty( actorTwo, "name", "Michael Nyquist");
          	sendTransactionalCypherQuery("MATCH (n { name: 'Michael Nyquist' }) SET n:Actor RETURN n");
          	URI relationshipUriFour = addRelationship( actorTwo, movieNode, "ACTS_IN", null);
          	addMetadataToProperty(relationshipUriFour, "role", "Viggo Tarasov");
            
          	// Create Reeves node if database is blank
          	//URI actorThree = createNode();
          	//addProperty( actorThree, "name", "Keanu Reeves");
          	//sendTransactionalCypherQuery("MATCH (n { name: 'Keanu Reeves' }) SET n:Actor RETURN n");
          		
          	sendTransactionalCypherQuery("MATCH (n { name: 'Keanu Reeves' }) "
          			+ "MATCH (p { title:'John Wick' }) "
          			+ "CREATE (n)-[r:ACTS_IN { role: 'John Wick'}]->(p) RETURN p,r,n");
            
            // query to see it all...
          	// MATCH (p)-[r]->(m:Movie)
          	// WHERE m.title = "John Wick" RETURN p,r,m
            
          	//CREATE (p)-[r:ACTED_IN { roles: ['Zachry']}]->(m) RETURN p,r,m
            
            // Cast of the movie included William Dafoe and Michael Nyquist
            // add label 
            //MATCH (n { name: 'Stefan' }) SET n :German RETURN n        
//          URI secondNode = createNode();
//          addProperty( secondNode, "band", "The Clash" );
//          
//          // END SNIPPET: nodesAndProps
//
//          // START SNIPPET: addRel
//          URI relationshipUri = addRelationship( firstNode, secondNode, "singer",
//                 "{ \"from\" : \"1976\", \"until\" : \"1986\" }" );
//          // MATCH (n {band:"The Clash"})<-[r]-(m) return n, r, m;
//          // END SNIPPET: addRel
//
//          // START SNIPPET: addMetaToRel
//           addMetadataToProperty( relationshipUri, "stars", "5" );
//          // END SNIPPET: addMetaToRel
//           findSingersInBands( firstNode );

           //sendTransactionalCypherQuery( "MATCH (n) WHERE has(n.name) RETURN n.name AS name" );
    }

    private static void sendTransactionalCypherQuery(String query) {
        // START SNIPPET: queryAllNodes
        final String txUri = SERVER_ROOT_URI + "transaction/commit";
        WebResource resource = Client.create().resource( txUri );

        String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( payload )
                .post( ClientResponse.class );
        
        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class ) ) );
        
        response.close();
        // END SNIPPET: queryAllNodes
    }

    private static void findSingersInBands( URI startNode )
            throws URISyntaxException
    {
        // START SNIPPET: traversalDesc
        // TraversalDefinition turns into JSON to send to the Server
        TraversalDefinition t = new TraversalDefinition();
        t.setOrder( TraversalDefinition.DEPTH_FIRST );
        t.setUniqueness( TraversalDefinition.NODE );
        t.setMaxDepth( 10 );
        t.setReturnFilter( TraversalDefinition.ALL );
        t.setRelationships( new Relation( "singer", Relation.OUT ) );
        // END SNIPPET: traversalDesc

        // START SNIPPET: traverse
        URI traverserUri = new URI( startNode.toString() + "/traverse/node" );
        WebResource resource = Client.create()
                .resource( traverserUri );
        String jsonTraverserPayload = t.toJson();
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( jsonTraverserPayload )
                .post( ClientResponse.class );

        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.lineSeparator() + "%s",
                jsonTraverserPayload, traverserUri, response.getStatus(),
                response.getEntity( String.class ) ) );
        response.close();
        // END SNIPPET: traverse
    }
    
    // START SNIPPET: insideAddMetaToProp
    private static void addMetadataToProperty( URI relationshipUri,
            String name, String value ) throws URISyntaxException
    {
        URI propertyUri = new URI( relationshipUri.toString() + "/properties" );
        String entity = toJsonNameValuePairCollection( name, value );
        WebResource resource = Client.create()
                .resource( propertyUri );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .put( ClientResponse.class );

        System.out.println( String.format(
                "PUT [%s] to [%s], status code [%d]", entity, propertyUri,
                response.getStatus() ) );
        response.close();
    }

    // END SNIPPET: insideAddMetaToProp

    private static String toJsonNameValuePairCollection( String name,
            String value )
    {
        return String.format( "{ \"%s\" : \"%s\" }", name, value );
    }

    private static URI createNode()
    {
        // START SNIPPET: createNode
    	// final String txUri = SERVER_ROOT_URI + "transaction/commit";
        final String nodeEntryPointUri = SERVER_ROOT_URI + "node";
        // http://localhost:7474/db/data/node

        WebResource resource = Client.create()
                .resource( nodeEntryPointUri );
        // POST {} to the node entry point URI
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( "{}" )
                .post( ClientResponse.class );

         final URI location = response.getLocation();
        System.out.println( String.format(
                "POST to [%s], status code [%d], location header [%s]",
                nodeEntryPointUri, response.getStatus(), location.toString()));
        response.close();

        return location;
        // END SNIPPET: createNode
    }

    // START SNIPPET: insideAddRel
    private static URI addRelationship( URI startNode, URI endNode,
            String relationshipType, String jsonAttributes )
            throws URISyntaxException
    {
        URI fromUri = new URI( startNode.toString() + "/relationships" );
        String relationshipJson = generateJsonRelationship( endNode,
                relationshipType, jsonAttributes );

        WebResource resource = Client.create()
                .resource( fromUri );
        // POST JSON to the relationships URI
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( relationshipJson )
                .post( ClientResponse.class );

        final URI location = response.getLocation();
        System.out.println( String.format(
                "POST to [%s], status code [%d], location header [%s]",
                fromUri, response.getStatus(), location.toString() ) );

        response.close();
        return location;
    }
    // END SNIPPET: insideAddRel

    private static String generateJsonRelationship( URI endNode,
            String relationshipType, String... jsonAttributes )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "{ \"to\" : \"" );
        sb.append( endNode.toString() );
        sb.append( "\", " );

        sb.append( "\"type\" : \"" );
        sb.append( relationshipType );
        if ( jsonAttributes == null || jsonAttributes.length < 1 )
        {
            sb.append( "\"" );
        }
        else
        {
            sb.append( "\", \"data\" : " );
            for ( int i = 0; i < jsonAttributes.length; i++ )
            {
                sb.append( jsonAttributes[i] );
                if ( i < jsonAttributes.length - 1 )
                { // Miss off the final comma
                    sb.append( ", " );
                }
            }
        }

        sb.append( " }" );
        return sb.toString();
    }

 
    
    private static void addProperty( URI nodeUri, String propertyName,
            String propertyValue )
    {
        // START SNIPPET: addProp
        String propertyUri = nodeUri.toString() + "/properties/" + propertyName;
        // http://localhost:7474/db/data/node/{node_id}/properties/{property_name}

        WebResource resource = Client.create()
                .resource( propertyUri );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( "\"" + propertyValue + "\"" )
                .put( ClientResponse.class );

        System.out.println( String.format( "PUT to [%s], status code [%d]",
                propertyUri, response.getStatus() ) );
        response.close();
        // END SNIPPET: addProp
    }

    private static void checkDatabaseIsRunning()
    {
        // START SNIPPET: checkServer
        WebResource resource = Client.create()
                .resource( SERVER_ROOT_URI );
        ClientResponse response = resource.get( ClientResponse.class );

        System.out.println( String.format( "GET on [%s], status code [%d]",
                SERVER_ROOT_URI, response.getStatus() ) );
        response.close();
        // END SNIPPET: checkServer
    }
}