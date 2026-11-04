"""Validate relationships retained in one locally fixed paper experiment.

This checks local consistency, not the authenticity or time of any record.
"""
import hashlib
import json


class FixedContractError(ValueError):
    pass


def validate_fixed_record(db, request_raw, digest, raw):
    try:
        def require(condition):
            if not condition:
                raise FixedContractError('固定实验请求、副本或引用不一致，需要复核')

        require(hashlib.sha256(raw).hexdigest() == digest)
        content = json.loads(raw)
        protocol = content['protocol']
        require(json.loads(request_raw) == protocol)
        manifests, pairs = protocol['replicas'], content['pairs']
        require(isinstance(manifests, list) and bool(manifests) and isinstance(pairs, list))
        require([(x['replica'], x['pair_key']) for x in pairs]
                == [(x['replica'], x['pair_key']) for x in manifests])
        require(len({x['replica'] for x in manifests}) == len(manifests)
                and len({x['pair_key'] for x in manifests}) == len(manifests))
        for kind in ('calendar', 'rule', 'tax'):
            review = db.execute('SELECT record_json FROM reviewed_sources WHERE id=?',
                                (protocol[kind + '_review_id'],)).fetchone()
            require(review and json.loads(review[0]) == content[kind + '_review'])
        calendar = content['calendar_review']['extracted']
        rule = content['rule_review']['extracted']
        require(calendar['issue'] == protocol['target_issue'])
        shared = None
        seeds, hashes = set(), set()
        for item in pairs:
            saved = db.execute('SELECT pair_json FROM draft_pairs WHERE request_key=?', (item['pair_key'],)).fetchone()
            value = item['pair']
            require(saved and json.loads(saved[0]) == value)
            config = value['config']
            require(config['target_issue'] == protocol['target_issue']
                    and config['target_date'] == calendar['draw_date']
                    and config['calendar_version'] == calendar['version']
                    and config['rule_version'] == rule['version'])
            profile = {key: value for key, value in config.items() if key != 'seed'}
            require(shared is None or shared == profile)
            shared = profile
            require(config['seed'] not in seeds and value['pair_sha256'] not in hashes)
            seeds.add(config['seed']); hashes.add(value['pair_sha256'])
        return content
    except FixedContractError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
        raise FixedContractError('固定实验内容或引用无效，需要复核') from exc
