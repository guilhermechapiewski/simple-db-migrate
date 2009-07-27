import os
import sys
import unittest

sys.path.insert(0, os.path.abspath("./src"))
sys.path.insert(0, os.path.abspath("./tests"))
sys.path.insert(0, os.path.abspath("../src"))
sys.path.insert(0, os.path.abspath("../tests"))

if __name__ == "__main__":
    from cli_test import *
    from config_test import *
    from core_test import *
    from helpers_test import *
    from main_test import *
    from mysql_test import *

    test_suites = []
    
    # add all tests to the test suite
    # TODO: eeek!!!! this MUST be done automatically
    # crap
    # crap
    # crap
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(CLITest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(ConfigTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(FileConfigTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(InPlaceConfigTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(ListsTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(MainTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(MySQLTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(MigrationTest))
    test_suites.append(unittest.TestLoader().loadTestsFromTestCase(MigrationsTest))
    
    alltests = unittest.TestSuite(test_suites)
    
    result = unittest.TestResult()
    alltests.run(result)
       
    if result.wasSuccessful():
        print "\n%s All %d tests passed :) %s\n" % ('*' * 20, result.testsRun, '*' * 20)
    else:
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
        print "\nError in tests (%d runned, %d errors, %d failures)\n" % (result.testsRun, len(result.errors), len(result.failures))
