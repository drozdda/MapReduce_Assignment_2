import multiprocessing
import collections
import itertools
import timeit
import string

class SimpleMapReduce(object):
    
    def __init__(self, map_func, reduce_func, num_workers=None):
       
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)
    
    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()
    
    def __call__(self, inputs, chunksize=1):
       
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values

def file_to_words(filename):
    
    STOP_WORDS = set([
            'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 'at', 'can'
            'is', 'it', 'of', 'or', 'on', 'our', 'that', 'the', 'to', 'with', 'we','s', 'they', 'will', 'us'
            ])
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

    print (multiprocessing.current_process().name, 'reading', filename)
    output = []

    with open(filename, 'rt') as f:
        for line in f:
            if line.lstrip().startswith('..'): 
                continue
            line = line.translate(TR) 
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS:
                    output.append( (word, 1) )
    return output


def count_words(item):
  
    word, occurances = item
    return (word, sum(occurances))


if __name__ == '__main__':
    import operator
    import glob
    import timeit
    input_files = glob.glob('kimmy*.txt')
    
    mapper = SimpleMapReduce(file_to_words, count_words)
    word_counts = mapper(input_files)
    word_counts.sort(key=operator.itemgetter(1))
    word_counts.reverse()
    
    print ('\nTOP 20 WORDS BY KIM JUNG UN\n')
    top20 = word_counts[:20]
    longest = max(len(word) for word, count in top20)
    for word, count in top20:
        print ('%-*s: %5s' % (longest+1, word, count))
