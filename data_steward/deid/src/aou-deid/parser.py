"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    This file is designed to put rules into a canonical form. 
    The framework distinguishes rules from their applications so as to allow simulation and have visibility into how the rules are applied given a variety of contexts

"""
import sys
class Parse:
    """
    This utility class implements the various ways in which rules and their applications are put into a canonical form
    Having rules in a canonical form makes it easy for an engine to apply them in batch
    """
    @staticmethod
    def init(id,row,cache):
        try:
            row_rules = row.get('rules', '')
            _id, _key = row_rules.replace('@', '').split('.')
        except ValueError:
            _id,_key = None,None

        label = ".".join([_id,_key]) if _key is not None else id
        
        if _id and _key  and _id in cache and _key in cache[_id]:
            p  =  {'label':label,'rules':cache[_id][_key]}
            
        else:
            p =  {'label':label}
        for key in row :
            p[key] = row[key] if key not in p else p[key]

        return p    
        
        

    @staticmethod
    def shift(row,cache):
        return Parse.init('shift',row,cache)

    @staticmethod
    def generalize(row,cache):
        """
        parsing generalization and translating the features into a canonical form of stors
        """
        p = Parse.init('generalize',row,cache)
        # if 'into' in row :
        #     p['fields'] = [row['into']]
        #     del p['into']
        # if 'on' in row :
        
        return p

    @staticmethod
    def suppress(row,cache):
        """
        setup suppression rules to be applied the the given 'row' i.e entry
        """
        return Parse.init('suppress',row,cache)

    @staticmethod
    def compute(row,cache):
        return Parse.init('compute',row,cache)

    @staticmethod
    def sys_args():
        SYS_ARGS = {}
        if len(sys.argv) > 1:
            
            N = len(sys.argv)
            for i in range(1,N):
                value = None
                if sys.argv[i].startswith('--'):
                    key = sys.argv[i].replace('-','')
                    SYS_ARGS[key] = 1
                    if i + 1 < N and sys.argv[i+1].startswith('--') is False:
                        value = sys.argv[i + 1] = sys.argv[i+1].strip()
                    if key and value:
                        SYS_ARGS[key] = value
                
                i += 2        
        return SYS_ARGS
#
# This part will parse system arguments
