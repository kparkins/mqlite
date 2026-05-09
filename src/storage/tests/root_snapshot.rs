use super::*;

fn mk_catalog() -> PublishedCatalog {
    let mut namespaces = HashMap::new();
    let mut by_name = HashMap::new();
    for (id, name) in [(1i64, "db.a"), (2i64, "db.b"), (7i64, "db.gap")] {
        namespaces.insert(
            id,
            NamespaceSnapshot {
                id,
                data_root_page: (id as u32) * 100,
                data_root_level: 0,
                indexes: Vec::new(),
            },
        );
        by_name.insert(name.to_owned(), id);
    }
    PublishedCatalog {
        namespaces,
        namespace_id_by_name: by_name,
        index_owner_by_id: HashMap::new(),
    }
}

/// The name sidecar resolves known names to their durable ids.
#[test]
fn namespace_sidecar_returns_id_for_known_name() {
    let cat = mk_catalog();
    assert_eq!(cat.namespace_id_by_name.get("db.a").copied(), Some(1));
    assert_eq!(cat.namespace_id_by_name.get("db.b").copied(), Some(2));
    assert_eq!(cat.namespace_id_by_name.get("db.gap").copied(), Some(7));
}

/// Unknown names are absent from the sidecar.
#[test]
fn namespace_sidecar_returns_none_for_unknown_name() {
    let cat = mk_catalog();
    assert_eq!(cat.namespace_id_by_name.get("db.missing"), None);
    assert_eq!(cat.namespace_id_by_name.get(""), None);
}

/// Name and id maps agree for the same namespace.
#[test]
fn namespace_sidecar_round_trips_through_namespaces_map() {
    let cat = mk_catalog();
    let id = cat
        .namespace_id_by_name
        .get("db.a")
        .copied()
        .expect("db.a resolves");
    let ns = cat.namespaces.get(&id).expect("id in namespaces map");
    assert_eq!(ns.id, id);

    let ns2 = cat.get_by_name("db.a").expect("name in sidecar");
    assert_eq!(ns2.id, id);
}
