import resource

def memory_limit(ratio):
    """ 
    Limits this processes system memory to a fixed ratio 
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (int(get_memory() * 1024 * ratio), hard))

def get_memory():
    """ 
    Calculates free system memory 

    Returns
    -------
    int
        free memory count

    """
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    return free_memory