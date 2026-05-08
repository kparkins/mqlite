use super::*;

fn mk_catalog() -> PublishedCatalog {
    let mut namespaces = HashMap::new();
    let mut by_name = HashMap::new();
    for (id, name) in [(1i64, "db.a"), (2i64, "db.b"), (7i64, "db.gap")] {
        namespaces.insert(
            id,
            NamespaceSnapshot {
                id,
                name: name.to_owned(),
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

/// §10.1 / §11 #11 — `id_for_name` resolves known names to their
/// durable id via the name sidecar.
#[test]
fn id_for_name_returns_sidecar_id_for_known_name() {
    let cat = mk_catalog();
    assert_eq!(cat.id_for_name("db.a"), Some(1));
    assert_eq!(cat.id_for_name("db.b"), Some(2));
    assert_eq!(cat.id_for_name("db.gap"), Some(7));
}

/// §11 #11 — `id_for_name` returns `None` for unknown names.
#[test]
fn id_for_name_returns_none_for_unknown_name() {
    let cat = mk_catalog();
    assert_eq!(cat.id_for_name("db.missing"), None);
    assert_eq!(cat.id_for_name(""), None);
}

/// `id_for_name` is consistent with `get_by_name` / `get_by_id`.
#[test]
fn id_for_name_round_trips_through_namespaces_map() {
    let cat = mk_catalog();
    let id = cat.id_for_name("db.a").expect("db.a resolves");
    let ns = cat.get_by_id(id).expect("id in namespaces map");
    assert_eq!(ns.name, "db.a");
    assert_eq!(ns.id, id);

    let ns2 = cat.get_by_name("db.a").expect("name in sidecar");
    assert_eq!(ns2.id, id);
}
