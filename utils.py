import argparse

def get_arguments_for_cleaning():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--nutch_path', type=str, default='/mnt/storage/crowl/adal_work/crawling/nutch',
                        help='The path  to your Apache Nutch')
    parser.add_argument('-j', '--java_env', type=str, default='/usr/lib/jvm/java-11-openjdk-amd64',
                        help='The path  to JAVA HOME')
    parser.add_argument('-d', '--dumps', type=str, default='nutch_core_segment_dumps',
                        help='The directory to save dumps')
    parser.add_argument('-c', '--csv', type=str, default='/mnt/storage/crowl/adal_work/two_k_links',
                        help='The directory to save CSV files')
    
    args = parser.parse_args()

    return args

if __name__=='__main__':
    args = get_arguments_for_cleaning()
    print(args)