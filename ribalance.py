import argparse

from pprint import pprint as pp
from datetime import datetime, timedelta
from collections import defaultdict as dd
from boto import ec2 as ec2m
from boto.ec2 import reservedinstance as rim

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
        return sorted(list(set(item.tags.get(tagname) for item in self.l)))

    def append(self, item):
        self.l.append(item)

    def __iter__(self):
        return iter(self.l)

    def __getitem__(self, val):
        if isinstance(val, slice):
            if isinstance(val.start, basestring) and isinstance(val.stop, basestring):
                # this is actually filtering for age using an age string
                return IL([i for i in self.l if val.start <= i.launch_time <= val.stop])
            elif isinstance(val.start, basestring):
                # pass
                return IL([i for i in self.l if val.start <= i.launch_time])
            elif isinstance(val.stop, basestring):
                # pass
                return IL([i for i in self.l if i.launch_time <= val.stop])
            return IL(self.l[val.start:val.stop:val.step])
        return self.l[val]

    def __len__(self):
        if self.l and hasattr(self.l[0], 'instance_count'):
            return sum(item.instance_count for item in self.l)
        return len(self.l)

    def sort(self, key=None):
        self.l.sort(key=key)

    def group_by_end_hour(self):
        # This is seriously the most bizarre requirement... In order to modify
        # RIs together they need to end in the same hour, not seconds or
        # minutes. Why even have this to begin with?
        o = dd(lambda : IL())
        for ri in self.l:
            o[self._get_ri_end_time(ri)].append(ri)
        return o

    def _get_ri_end_time(self, ri):
        return datetime.strptime(ri.end.split(':', 1)[0], "%Y-%m-%dT%H").isoformat()

    def _get_platform(self, ii_or_ri):
        if hasattr(ii_or_ri, 'description'):
            if 'VPC' in ii_or_ri.description:
                return 'EC2-VPC'
            return 'EC2-Classic'
        if ii_or_ri.vpc_id:
            return 'EC2-VPC'
        return 'EC2-Classic'

    def group_by_zone_and_plat(self):
        zones = dd(lambda: dd(lambda : IL()))
        for i in self.l:
            if hasattr(i, 'placement'):
                zones[self._get_platform(i)][i.placement].append(i)
            elif hasattr(i, 'availability_zone'):
                zones[self._get_platform(i)][i.availability_zone].append(i)
            else:
                raise TypeError("unknown object type %s" % (type(i),))
        return zones

    @property
    def ids(self):
        return [i.id for i in self.l]

def conf_target_to_dict(tc):
    return {"az": tc.availability_zone,
            "platform": tc.platform,
            "instance_count": tc.instance_count,
            "instance_type": tc.instance_type}

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

            if s+t.instance_count < idx:
                s += t.instance_count

            elif s+t.instance_count == idx:
                cidx += 1 # This split was fine, next one.
                s += t.instance_count

            else: # s + t.instance_count > idx
                # if we exceed the idx in this loop, let's split the target
                # config at this point in 2, and proceed to the next split
                # I'm inserting after the current split because if I might
                # need to split again before the end of this TargetConfig.
                # I'm also not adding the whole thing to s either, just as much
                # as I used in the first step which is pre_size.
                post_size = s + t.instance_count - idx
                pre_size = t.instance_count - post_size

                t.instance_count = pre_size
                self.l.insert(i+1, rim.ReservedInstancesConfiguration(
                        availability_zone=t.availability_zone,
                        platform=t.platform,
                        instance_count=post_size,
                        instance_type=t.instance_type
                ))
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
                # No changes to self.l, ideally we've already called
                # make_splits_at
                if not found_start and start+t.instance_count <= val.start:
                    start += t.instance_count
                    if start == val.start:
                        found_start = True
                    continue

                if start+t.instance_count < stop:
                    out.append(t)
                    start += t.instance_count

                elif start+t.instance_count == stop:
                    out.append(t)
                    return out

                elif start+t.instance_count > stop:
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
    targets.sort(key=lambda o: o.availability_zone)
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
        self.ec2 = ec2m.connect_to_region(region)

        for reserved in self.ec2.get_all_reserved_instances():
            if reserved.state != "active":
                continue
            self.addri(reserved.instance_type, reserved.availability_zone, reserved)

        for reservation in self.ec2.get_all_instances():
            for instance in reservation.instances:
                if instance.state != 'running':
                    continue
                if instance.spot_instance_request_id:
                    self.adds(instance.instance_type, instance.placement, instance)
                else:
                    self.addi(instance.instance_type, instance.placement, instance)

    def addri(self, type_, zone, ri):
        self.instances[type_]["ri"].append(ri)

    def addi(self, type_, zone, ii):
        self.instances[type_]['iis'].append(ii)

    def adds(self, type_, zone, spot):
        self.instances[type_]['spot'].append(spot)

    def ideal_target(self, age, tag, commit=False):
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
        age_string = age.isoformat().rsplit('.', 1)[0] + ".000Z"
        changes = dd(lambda :dd(lambda: dd(lambda : [])))
        recs = dd(lambda :dd(lambda: dd(lambda : [])))
        ideal_target = {}
        regional_situation = dd(lambda :dd(lambda: dd(lambda : {})))
        mods = []
        for type_, instmap in self.instances.iteritems():
            target_configurations = []

            total_ris = len(instmap["ri"])

            # deserving_instmap are all instmap that have priority in being
            # covered by a reserved instance.
            instmap['iis'].sort(lambda i: i.launch_time)
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
                    regional_situation[type_][zone][platform]["OnDemand Sustained"] = len(grouped_iis[platform][zone][:age_string])
                    regional_situation[type_][zone][platform]["Tags"] = grouped_iis[platform][zone].tags(tag)


                    if num_ris != num_instances:
                        changes[type_][platform][zone].append(num_instances-num_ris)

                    if num_instances+pad <= 0:
                        # Instance count can't be negative or 0.
                        print "Skipping zero or negative instance count...", region, type_
                        continue
                    target_configurations.append(rim.ReservedInstancesConfiguration(
                       availability_zone=zone,
                       platform=platform,
                       instance_count=num_instances+pad,
                       instance_type=type_
                    ))
                    pad = 0

            # Save the target configuration
            ideal_target[type_] = target_configurations

            # If asked to commit it, do it
            if commit:
                if should_execute(changes[type_]) and instmap["ri"].ids:
                    try:
                        grouped_ris_and_targets = match_targets(instmap["ri"].group_by_end_hour(), target_configurations)
                        for hour, grouped_ris, grouped_targets in grouped_ris_and_targets:
                            client_token = ".".join([
                                datetime.utcnow().replace(minute=0, second=0, microsecond=0).isoformat(),
                                type_,
                                hour
                            ])

                            mods.append((type_, self.ec2.modify_reserved_instances(
                                client_token=client_token,
                                reserved_instance_ids=grouped_ris.ids,
                                target_configurations=grouped_targets
                            )))
                            # mods.append((type_, dict(
                            #     client_token=client_token,
                            #     reserved_instance_ids=grouped_ris.ids,
                            #     num_reserved_instance=len(grouped_ris),
                            #     target_configurations=[conf_target_to_dict(tc)
                            #                            for tc in
                            #                            grouped_targets]
                            # )))
                    except Exception, e:
                        print "Error", region, type_, e
                        if "Invalid value for 'clientToken'" in e.message:
                            continue
                        raise
                else:
                    print "Skipping...", region, type_


            # Try to guess what would be a good new purchase of RIs
            rest = instmap['iis'][total_ris:]
            if not rest:
                continue

            new_suggested = rest[:age_string]

            if not new_suggested:
                continue

            suggested = new_suggested.group_by_zone_and_plat()
            for platform, zones in suggested.iteritems():
                for zone, insts in zones.iteritems():
                    recs[type_][platform][zone].append("+R:%s" % len(insts))
                    recs[type_][platform][zone].append(insts.tags(tag))


        return changes, ideal_target, regional_situation, recs, mods

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ribalance will try to allocate your RIs in the most efficient way.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    dr = ec2m.RegionData.keys()
    dr.remove('cn-north-1')
    dr.remove('us-gov-west-1')

    parser.add_argument('--regions', nargs='*', default=dr,
                        choices=ec2m.RegionData.keys(), help='Regions to apply this')
    parser.add_argument('--commit', action='store_true', help='Should apply changes') 
    parser.add_argument('--changes', action='store_true', help='Display changes')
    parser.add_argument('--recs', action='store_true', help='Display recommendations on new RI purchases')
    parser.add_argument('--target', action='store_true', help='Display target')
    parser.add_argument('--state', action='store_true', help='Display state')
    parser.add_argument('--tag', type=str, default="application", help='Name of the tag that splits the different applications')
    parser.add_argument('--age', type=int, default=168, help='Hours since launch to consider sustained (1wk default)')

    args = parser.parse_args()

    res_age = datetime.utcnow() - timedelta(hours=args.age)
    for region in args.regions:
        print "=============  " + region + "  =============="
        rrimap = RegionalMap(region)
        changes, targets, regional_situation, recs, mods = rrimap.ideal_target(res_age, args.tag, args.commit)
        if args.changes:
            print "changes"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(changes).items()})
        if args.recs:
            print "recs"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(recs).items()})
        if args.target:
            print "targets"
            pp({k: [conf_target_to_dict(i) for i in v] for k, v in targets.items()})
        if args.state:
            print "regional breakdown"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(regional_situation).items()})
        if args.commit:
            print "RI Modifications"
            pp(mods)

