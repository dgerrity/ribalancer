import boto3
import argparse

from pprint import pprint as pp
from dateutil.tz import tzutc
from datetime import datetime, timedelta
from collections import defaultdict as dd

class IL(object):
    """
    This object represents an Instance List and it provides a set of
    handy arguments that operate on the list of instances or reserved instances
    that is contained in the list.

    This object also tries to simulate the list API, although it's not a real
    list.
    """
    def __init__(self, l=None):
        self.l = l
        if l is None:
            self.l = []

    def tags(self, tagname):
        out = []
        for item in self.l:
            for tag in item.get('Tags', []):
                if tag.get('Key') == tagname:
                    out.append(tag.get('Value'))
                    break
            else:
                out.append(None)
        return sorted(set(out))

    def append(self, item):
        self.l.append(item)

    def __iter__(self):
        return iter(self.l)

    def __getitem__(self, val):
        if isinstance(val, slice):
            if isinstance(val.start, datetime) and isinstance(val.stop, datetime):
                return IL([i for i in self.l if val.start <= i.get('LaunchTime') <= val.stop])
            elif isinstance(val.start, datetime):
                return IL([i for i in self.l if val.start <= i.get('LaunchTime')])
            elif isinstance(val.stop, datetime):
                return IL([i for i in self.l if i.get('LaunchTime') <= val.stop])
            return IL(self.l[val.start:val.stop:val.step])
        return self.l[val]

    def __len__(self):
        if self.l and 'InstanceCount' in self.l[0]:
            return sum(item.get('InstanceCount') for item in self.l)
        return len(self.l)

    def sort(self, key=None):
        self.l.sort(key=key)

    def group_by_end_hour(self):
        # This is seriously the most bizarre requirement... In order to modify
        # RIs together they need to end in the same hour, not seconds or
        # minutes. Why even have this to begin with?
        o = dd(lambda : IL())
        for ri in self.l:
            o[ri.get('End')].append(ri)
        return o

    def _get_platform(self, ii_or_ri):
        description = ii_or_ri.get('ProductDescription')
        if description is not None:
            if 'VPC' in description:
                return 'EC2-VPC'
            return 'EC2-Classic'
        if ii_or_ri.get('VpcId'):
            return 'EC2-VPC'
        return 'EC2-Classic'

    def group_by_zone_and_plat(self):
        zones = dd(lambda: dd(lambda : IL()))
        for i in self.l:
            if 'Placement' in i:
                zones[self._get_platform(i)][i.get('Placement').get('AvailabilityZone')].append(i)
            elif 'AvailabilityZone' in i:
                zones[self._get_platform(i)][i.get('AvailabilityZone')].append(i)
            else:
                raise TypeError("unknown object type %s" % (i,))
        return zones

    @property
    def ids(self):
        return [i.get('ReservedInstancesId') or i.get('InstanceId') for i in self.l]

def should_execute(changes):
    if not changes:
        return False
    for platform, zones in changes.iteritems():
        for zone, provision in zones.iteritems():
            if any(p>0 for p in provision):
                return True
    return False

class TargetConfigSlicer(object):
    def __init__(self, l=None):
        self.l = l
        if l is None:
            self.l = []

    def make_splits_at(self, idxs):
        s = 0
        i = 0
        cidx = 0
        while i < len(self.l) and cidx < len(idxs):
            idx = idxs[cidx]
            t = self.l[i]

            instance_count = t.get('InstanceCount')
            if s+instance_count < idx:
                s += instance_count

            elif s+instance_count == idx:
                cidx += 1 # This split was fine, next one.
                s += instance_count

            else: # s + t.instance_count > idx
                # if we exceed the idx in this loop, let's split the target
                # config at this point in 2, and proceed to the next split
                # I'm inserting after the current split because if I might
                # need to split again before the end of this TargetConfig.
                # I'm also not adding the whole thing to s either, just as much
                # as I used in the first step which is pre_size.
                post_size = s + instance_count - idx
                pre_size = instance_count - post_size

                t['InstanceCount'] = pre_size
                new_t = dict(t)
                new_t['InstanceCount'] = post_size
                self.l.insert(i+1, new_t)
                cidx += 1
                s += pre_size

            i += 1

    def __getitem__(self, val):
        if isinstance(val, slice):
            out = []
            i = 0
            aggr = 0
            start = 0
            stop = val.stop or len(self)
            found_start = False
            for t in self.l:
                instance_count = t.get('InstanceCount')
                # No changes to self.l, ideally we've already called
                # make_splits_at
                if not found_start and start+instance_count <= val.start:
                    start += instance_count
                    if start == val.start:
                        found_start = True
                    continue

                if start+instance_count < stop:
                    out.append(t)
                    start += instance_count

                elif start+instance_count == stop:
                    out.append(t)
                    return out

                elif start+instance_count > stop:
                    raise Exception("Have you called make_splits_at?")

        return self.l[val]


    def __len__(self):
        return sum(i.instance_count for i in self.l)


def match_targets(ris, targets):
    """
    ri is a map
        {end_time: [ri, ri, ri],
         end_time2: [ri, ri, ri]}
    targets is a list:
        [tc1, tc2, tc3]

    each individual item here underlies more than 1 item. The point here is to
    align each group of reservations with the group of tcs.

    Basically we need a list object that supports slicing through this list and
    knows how to generate new objects of a certain kind.
    """
    targets.sort(key=lambda o: o.get('AvailabilityZone'))
    targets = TargetConfigSlicer(targets)

    splits = []
    aggr = 0
    for _, v in ris.items():
        splits.append(len(v)+aggr)
        aggr += len(v)
    targets.make_splits_at(splits)

    out = []
    s = 0
    for hour, ri_group in ris.iteritems():
        target_group = targets[s:s+len(ri_group)]
        s += len(ri_group)

        out.append((hour, ri_group, target_group))

    return out

class RegionalMap(object):
    """
    This object represents the structure of a region in terms of instances,
    spots and reserved instances
    """

    def __init__(self, region):
        self.instances = dd(lambda: dd(lambda : IL()))
        self.ec2 = boto3.client('ec2', region)

        for reserved in self.ec2.describe_reserved_instances().get('ReservedInstances', []):
            if reserved['State'] != "active":
                continue
            self.addri(reserved['InstanceType'], reserved['AvailabilityZone'], reserved)

        for reservation in self.ec2.describe_instances().get('Reservations', []):
            for instance in reservation['Instances']:
                if instance['State']['Name'] != 'running':
                    continue
                if instance.get('InstanceLifecycle') == "spot":
                    self.adds(instance['InstanceType'], instance['Placement']['AvailabilityZone'], instance)
                else:
                    self.addi(instance['InstanceType'], instance['Placement']['AvailabilityZone'], instance)

    def addri(self, type_, zone, ri):
        self.instances[type_]["ri"].append(ri)

    def addi(self, type_, zone, ii):
        self.instances[type_]['iis'].append(ii)

    def adds(self, type_, zone, spot):
        self.instances[type_]['spot'].append(spot)

    def commit(self):
        mods = []
        for type_, instmap in self.instances.iteritems():
            if should_execute(self.changes[type_]) and instmap["ri"].ids:
                try:
                    grouped_ris_and_targets = match_targets(instmap["ri"].group_by_end_hour(), self.ideal_target[type_])
                    for hour, grouped_ris, grouped_targets in grouped_ris_and_targets:
                        client_token = ".".join([
                            datetime.utcnow().replace(minute=0, second=0, microsecond=0).isoformat(),
                            type_,
                            hour.isoformat()
                        ])

                        mods.append((type_,
                                     self.ec2.modify_reserved_instances(
                                        ClientToken=client_token,
                                        ReservedInstancesIds=grouped_ris.ids,
                                        TargetConfigurations=grouped_targets
                                     )
                                    )
                        )
                except Exception, e:
                    print "Error", region, type_, e
                    if "Invalid value for 'clientToken'" in e.message:
                        continue
                    raise
            else:
                print "Skipping...", region, type_
        return mods

    @property
    def proposed_changes(self):
        return self.changes

    @property
    def current_state(self):
        return self.state

    @property
    def new_ri_recommendations(self):
        return self.recs

    @property
    def target_configurations(self):
        return self.ideal_target


    def ideal_target(self, age, tag):
        """
        This is a relatively simple algorithm.

        Count how many RIs are available in a region for a size,
        then sorts instances of that size in the region by start date asc,
        then cuts the list at the number of RIs and gets the zone and count,
        and that would be the recommended layout

        For the remainder if we're running an instance for longer than a given
        period of time, it considers the instance a missing RI and suggests the
        purchase.

        The return value of the function is a dictionary of

        instance_size -> zone -> [net change, new suggested reservations]

        if the total result of a region is a negative net change it means that
        there's too many reserved instances.
        """
        self.changes = changes = dd(lambda :dd(lambda: dd(lambda : [])))
        self.recs = recs = dd(lambda :dd(lambda: dd(lambda : [])))
        self.ideal_target = ideal_target = {}
        self.state = regional_situation = dd(lambda :dd(lambda: dd(lambda : {})))
        for type_, instmap in self.instances.iteritems():
            target_configurations = []

            total_ris = len(instmap["ri"])

            # deserving_instmap are all instmap that have priority in being
            # covered by a reserved instance.
            instmap['iis'].sort(lambda i: i.get('LaunchTime'))
            deserving_instmap = instmap['iis'][:total_ris]

            total_iis = len(instmap['iis'])
            total_deserving = len(deserving_instmap)

            # And here we begin to find coverage holes.
            grouped_instmap = deserving_instmap.group_by_zone_and_plat()
            grouped_ris = instmap["ri"].group_by_zone_and_plat()
            grouped_iis = instmap['iis'].group_by_zone_and_plat()

            # This is very useful when there are more RIs than availabilable
            # instmap. You don't quite know where to place the excess RIs
            # and the current algorithm chooses to place all of them in the
            # first zone that we iterate on.
            pad = 0
            if total_ris > len(instmap['iis']):
                pad = total_ris - len(instmap['iis'])

            for platform in sorted(set(grouped_instmap.keys() + grouped_ris.keys())):
                for zone in sorted(set(grouped_instmap[platform].keys() +
                                       grouped_ris[platform].keys())):
                    instances = grouped_instmap[platform][zone]
                    ris = grouped_ris[platform][zone]

                    num_instances = len(instances)
                    num_ris = len(ris)

                    regional_situation[type_][zone][platform]["RI"] = num_ris
                    regional_situation[type_][zone][platform]["OnDemand"] = len(grouped_iis[platform][zone])
                    regional_situation[type_][zone][platform]["OnDemand Sustained"] = len(grouped_iis[platform][zone][:age])
                    regional_situation[type_][zone][platform]["Tags"] = grouped_iis[platform][zone].tags(tag)


                    if num_ris != num_instances:
                        changes[type_][platform][zone].append(num_instances-num_ris)

                    if num_instances+pad <= 0:
                        # Instance count can't be negative or 0.
                        print "Skipping zero or negative instance count...", region, type_
                        continue

                    target_configurations.append(dict(
                       AvailabilityZone=zone,
                       Platform=platform,
                       InstanceCount=num_instances+pad,
                       InstanceType=type_
                    ))
                    pad = 0

            # Save the target configuration
            ideal_target[type_] = target_configurations

            # Try to guess what would be a good new purchase of RIs
            rest = instmap['iis'][total_ris:]
            if not rest:
                continue

            new_suggested = rest[:age]

            if not new_suggested:
                continue

            suggested = new_suggested.group_by_zone_and_plat()
            for platform, zones in suggested.iteritems():
                for zone, insts in zones.iteritems():
                    recs[type_][platform][zone].append("+R:%s" % len(insts))
                    recs[type_][platform][zone].append(insts.tags(tag))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ribalance will try to allocate your RIs in the most efficient way.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    regions = ['us-east-1', 'cn-north-1', 'ap-northeast-1', 'ap-southeast-2',
               'sa-east-1', 'ap-southeast-1', 'ap-northeast-2', 'us-west-2',
               'us-gov-west-1', 'us-west-1', 'eu-central-1', 'eu-west-1']
    dr = list(regions)
    dr.remove('cn-north-1')
    dr.remove('us-gov-west-1')


    parser.add_argument('--regions', nargs='*', default=dr, choices=regions, help='Regions to apply this')
    parser.add_argument('--commit', action='store_true', help='Should apply changes') 
    parser.add_argument('--changes', action='store_true', help='Display changes')
    parser.add_argument('--recs', action='store_true', help='Display recommendations on new RI purchases')
    parser.add_argument('--target', action='store_true', help='Display target')
    parser.add_argument('--state', action='store_true', help='Display state')
    parser.add_argument('--tag', type=str, default="application", help='Name of the tag that splits the different applications')
    parser.add_argument('--age', type=int, default=168, help='Hours since launch to consider sustained (1wk default)')

    args = parser.parse_args()

    res_age = datetime.utcnow() - timedelta(hours=args.age)
    res_age = res_age.replace(tzinfo=tzutc())
    for region in args.regions:
        print "=============  " + region + "  =============="
        rrimap = RegionalMap(region)
        rrimap.ideal_target(res_age, args.tag)
        if args.changes:
            print "changes"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(rrimap.proposed_changes).items()})
        if args.recs:
            print "recs"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(rrimap.new_ri_recommendations).items()})
        if args.target:
            print "targets"
            pp(rrimap.target_configurations)
        if args.state:
            print "regional breakdown"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(rrimap.state).items()})
        if args.commit:
            print "RI Modifications"
            pp(rrimap.commit())

