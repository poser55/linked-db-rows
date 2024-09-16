package org.oser.tools.jdbc;

import org.flywaydb.core.internal.jdbc.DriverDataSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class KeycloakTests {

    // start postgres & keycloak:
    // docker.exe run -d --name postgres -e POSTGRES_DB=marcopolo-dev -e POSTGRES_USER=marcopolo-dev -e POSTGRES_PASSWORD=password -p 5431:5432 postgres
    // docker.exe exec -it postgres psql -U marcopolo-dev -d marcopolo-dev -c "CREATE SCHEMA IF NOT EXISTS keycloak"
    // docker.exe run --name keycloak -e DB_ADDR=host.docker.internal -e DB_VENDOR=POSTGRES -e DB_DATABASE=marcopolo-dev -e DB_USER=marcopolo-dev -e KEYCLOAK_USER=admin -e DB_PORT=5431 -e KEYCLOAK_PASSWORD=admin -p 8080:8080 jboss/keycloak
    // go to http://localhost:8080/ to go to the console of keycloak


    // newer
    //  docker.exe run --name keycloak -e DB_ADDR=host.docker.internal -e DB_VENDOR=POSTGRES -e DB_DATABASE=postgres -e DB_USER=postgres -e KEYCLOAK_USER=admin -e DB_PORT=5432 -e KEYCLOAK_PASSWORD=admin -e DB_PASSWORD=admin -p 8080:8080 jboss/keycloak

    @Test
    @Disabled // needs keycloak setup
    void exportFullUserKeycloak() throws Exception {
        Class.forName("org.postgresql.Driver", true, Thread.currentThread().getContextClassLoader());

        DriverDataSource ds = new DriverDataSource(Thread.currentThread().getContextClassLoader(),
                "org.postgresql.Driver", "jdbc:postgresql://host.docker.internal:5431/marcopolo-dev?currentSchema=public",
                "marcopolo-dev", "password");
        Connection con = ds.getConnection();

        assertNotNull(con);

        DbExporter dbExporter = new DbExporter();
        // step 1
        Fk.addVirtualForeignKey(con, dbExporter, "user_role_mapping", "role_id", "keycloak_role", "id" );
        // step 2 (remove too much details)
        dbExporter.getStopTablesIncluded().add("keycloak_role");
        // todo: you need to adapt UUID to your environment
        DbRecord exportedCase = dbExporter.contentAsTree(con, "user_entity", "98831e8d-898a-4b6c-bc5c-e10ac3911380");

        Set<RowLink> allNodes = exportedCase.getAllNodes();
        System.out.println("exportedPerson.size():"+ allNodes.size()+"\n"+ allNodes+"\n\n\n");

        String caseAsJson = exportedCase.asJsonNode().toString();

        System.out.println(caseAsJson+"\n\n"+exportedCase.getAllNodes().size());
    }
}
