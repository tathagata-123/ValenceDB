#include "test_harness.h"
#include "valencedb/common/version.h"

#include <iostream>
#include <string>

int main() {
    try {
        VDB_CHECK(valencedb::ProjectName() == "ValenceDB");

        // Verify that a failed assertion reports the expression and location.
        bool failure_detected = false;
        try {
            VDB_CHECK(2 + 2 == 5);
        } catch (const valencedb::test::CheckFailure& error) {
            const std::string message = error.what();
            failure_detected = message.find("2 + 2 == 5") != std::string::npos &&
                               message.find("smoke_test.cpp:") != std::string::npos;
        }
        VDB_CHECK(failure_detected);
        std::cout << "ValenceDB foundation smoke test passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
