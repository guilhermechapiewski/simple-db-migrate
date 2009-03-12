import os
import sys
import unittest

sys.path.insert(0, os.path.abspath("./src/simple_db_migrate"))
sys.path.insert(0, os.path.abspath("./tests"))
sys.path.insert(0, os.path.abspath("../src/simple_db_migrate"))
sys.path.insert(0, os.path.abspath("../tests"))

if __name__ == "__main__":
    from cli_test import *
    from core_test import *
    from helpers_test import *
    from main_test import *
    from mysql_test import *

    test_suites = []
    
    # add all tests to the test suite
    # TODO: this could be done automatically
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(CLITest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(ListsTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(SimpleDBMigrateTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(MainTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(MySQLTest))
    
    alltests = unittest.TestSuite(test_suites)
    
    result = unittest.TestResult()
    alltests.run(result)
       
    if result.wasSuccessful():
        print "\n*** All %d tests passed :) ***\n" % result.testsRun
    else:
        print "\nError in tests (%d runned, %d errors, %d failures)\n" % (result.testsRun, len(result.errors), len(result.failures))

        for problems in [result.errors, result.failures]:
            for problem in problems:
                print "======================================================================"
                i = 0
                for info in problem:
                    if i == 1:
                        print "----------------------------------------------------------------------"
                    if i == 0:
                        print "FAIL: %s" % info
                    else:
                        print info
                    i += 1
                print "----------------------------------------------------------------------"
        print ""
