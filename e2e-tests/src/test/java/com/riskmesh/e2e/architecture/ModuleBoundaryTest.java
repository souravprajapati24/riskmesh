package com.riskmesh.e2e.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

class ModuleBoundaryTest {

    private static final JavaClasses ALL_CLASSES =
            new ClassFileImporter().importPackages("com.riskmesh");

    @Test
    void ingestionServiceDoesNotDependOnOtherServiceModules() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("com.riskmesh.ingestion..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage(
                                "com.riskmesh.scoring..",
                                "com.riskmesh.casemanagement..",
                                "com.riskmesh.notification..");
        rule.check(ALL_CLASSES);
    }

    @Test
    void scoringServiceDoesNotDependOnOtherServiceModules() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("com.riskmesh.scoring..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage(
                                "com.riskmesh.ingestion..",
                                "com.riskmesh.casemanagement..",
                                "com.riskmesh.notification..");
        rule.check(ALL_CLASSES);
    }

    @Test
    void caseManagementServiceDoesNotDependOnOtherServiceModules() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("com.riskmesh.casemanagement..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage(
                                "com.riskmesh.ingestion..",
                                "com.riskmesh.scoring..",
                                "com.riskmesh.notification..");
        rule.check(ALL_CLASSES);
    }

    @Test
    void notificationServiceDoesNotDependOnOtherServiceModules() {
        ArchRule rule =
                noClasses()
                        .that()
                        .resideInAPackage("com.riskmesh.notification..")
                        .should()
                        .dependOnClassesThat()
                        .resideInAnyPackage(
                                "com.riskmesh.ingestion..",
                                "com.riskmesh.scoring..",
                                "com.riskmesh.casemanagement..");
        rule.check(ALL_CLASSES);
    }
}
