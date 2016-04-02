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
        return set(item.tags.get(tagname) for item in self.l)

    def append(self, item):
        self.l.append(item)

    def __iter__(self):
        return iter(self.l)

    def __getitem__(self, val):
        if isinstance(val, slice):
            return IL(self.l[val.start:val.stop:val.step])
        return self.l[val]

    def __len__(self):
        if self.l and hasattr(self.l[0], 'instance_count'):
            return sum(item.instance_count for item in self.l)
        return len(self.l)

    def sort(self, key=None):
        self.l.sort(key=key)

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
            # Typically if there are other fields, they are added in a following
            # step of the calling function. The actual recommendation is the
            # first number in the list, and of course if they are all negative
            # (meaning we should remove RIs) it means there's no point in moving
            # things, so this checks that there's at least 1 positive value.
            if provision[0] > 0:
                return True
    return False

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

    def ideal_target(self, age, commit=False):
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
        changes = dd(lambda :dd(lambda: dd(lambda : [])))
        ideal_target = {}
        regional_situation = {}
        mods = []
        for type_, instmap in self.instances.iteritems():
            target_configurations = []
            regional_type_situation = {}

            total_ris = len(instmap["ri"])

            # deserving_instmap are all instmap that have priority in being
            # covered by a reserved instance.
            instmap['iis'].sort(lambda i: i.launch_time)
            deserving_instmap = instmap['iis'][:total_ris]

            total_iis = len(instmap['iis'])
            total_deserving = len(deserving_instmap)
            regional_type_situation["Total RI"] = total_ris
            regional_type_situation["Total On Demand Sustained"] = total_deserving
            regional_type_situation["Total On Demand"] = total_iis

            regional_situation[type_] = regional_type_situation

            # And here we begin to find coverage holes.
            grouped_instmap = deserving_instmap.group_by_zone_and_plat()
            grouped_ris = instmap["ri"].group_by_zone_and_plat()

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

                    if num_ris != num_instances:
                        changes[type_][platform][zone].append(num_instances-num_ris)

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
                    client_token = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
                    mods.append((type_, self.ec2.modify_reserved_instances(
                        client_token=client_token,
                        reserved_instance_ids=instmap["ri"].ids,
                        target_configurations=target_configurations
                    )))
                else:
                    print "Skipping...", region, type_


            # Try to guess what would be a good new purchase of RIs
            rest = instmap['iis'][total_ris:]
            if not rest:
                continue

            age_string = age.isoformat().rsplit('.', 1)[0] + ".000Z"
            new_suggested = IL([i for i in rest if i.launch_time <= age_string])

            if not new_suggested:
                continue

            suggested = new_suggested.group_by_zone_and_plat()
            for platform, zones in suggested.iteritems():
                for zone, insts in zones.iteritems():
                    changes[type_][platform][zone].append("+R:%s" % len(new_suggested))


        return changes, ideal_target, regional_situation, mods

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
    parser.add_argument('--target', action='store_true', help='Display target')
    parser.add_argument('--state', action='store_true', help='Display state')
    parser.add_argument('--age', type=int, default=168, help='Hours since launch to consider sustained (1wk default)')



    args = parser.parse_args()

    res_age = datetime.utcnow() - timedelta(hours=args.age)
    for region in args.regions:
        print "=============  " + region + "  =============="
        rrimap = RegionalMap(region)
        changes, targets, regional_situation, mods = rrimap.ideal_target(res_age, args.commit)
        if args.changes:
            print "changes"
            pp({k: dict({k1: dict(v1) for k1, v1 in v.items()}) for k, v in dict(changes).items()})
        if args.target:
            print "targets"
            pp({k: [conf_target_to_dict(i) for i in v] for k, v in targets.items()})
        if args.state:
            print "regional breakdown"
            pp(regional_situation)
        if args.commit:
            print "RI Modifications"
            pp(mods)

